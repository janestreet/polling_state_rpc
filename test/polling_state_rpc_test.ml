open! Core
open! Async

module T = struct
  type t =
    { foo : string
    ; bar : int
    }
  [@@deriving sexp, diff, bin_io]
end

module T_incompatible = struct
  type t =
    { foo : string
    ; bar : int
    ; added_field : int
    }
  [@@deriving sexp, diff, bin_io]
end

let rpc =
  Polling_state_rpc.create
    ~name:"foo"
    ~version:0
    ~query_equal:[%equal: string]
    ~bin_query:[%bin_type_class: string]
    (module T)
;;

let incompatible_rpc =
  Polling_state_rpc.create
    ~name:"foo"
    ~version:0
    ~query_equal:[%equal: string]
    ~bin_query:[%bin_type_class: string]
    (module T_incompatible)
;;

let where_to_listen_for_unit_test =
  Tcp.Where_to_listen.bind_to Localhost On_port_chosen_by_os
;;

let make_server ?server_received_request ?(block = fun () -> Deferred.unit) () =
  let implementation =
    let items : T.t list =
      [ { foo = "abc"; bar = 32 }; { foo = "def"; bar = 25 }; { foo = "ghi"; bar = 22 } ]
    in
    let number_of_queries = ref 0 in
    let implementation _ query =
      let count = !number_of_queries in
      Int.incr number_of_queries;
      let found = List.find_exn items ~f:(fun { foo; _ } -> String.equal foo query) in
      Deferred.return { found with bar = found.bar + count }
    in
    Polling_state_rpc.implement
      rpc
      ~on_client_and_server_out_of_sync:
        (Expect_test_helpers_core.print_s ~hide_positions:true)
      ~for_first_request:implementation
      (fun _ query ->
         let%bind () =
           match server_received_request with
           | Some server_received_request -> Mvar.put server_received_request ()
           | None -> return ()
         in
         let%bind () = block () in
         implementation () query)
  in
  let open Deferred.Or_error.Let_syntax in
  let%map server =
    Deferred.ok
    @@ Rpc.Connection.serve
         ~implementations:
           (Rpc.Implementations.create_exn
              ~implementations:[ implementation ]
              ~on_unknown_rpc:`Close_connection)
         ~initial_connection_state:(fun _addr conn -> (), conn)
         ~where_to_listen:where_to_listen_for_unit_test
         ()
  in
  server, Tcp.Where_to_connect.of_inet_address (Tcp.Server.listening_on_address server)
;;

let make_client ?initial_query () =
  let introspect prev query diff =
    print_s
      [%message
        (prev : T.t option)
          (query : string)
          (diff : T.t Polling_state_rpc.Private_for_testing.Response.t)]
  in
  Polling_state_rpc.Private_for_testing.create_client ?initial_query rpc ~introspect
;;

let make_incompatible_client ?initial_query () =
  let introspect prev query diff =
    print_s
      [%message
        (prev : T_incompatible.t option)
          (query : string)
          (diff : T_incompatible.t Polling_state_rpc.Private_for_testing.Response.t)]
  in
  Polling_state_rpc.Private_for_testing.create_client
    ?initial_query
    incompatible_rpc
    ~introspect
;;

let%expect_test "basic operations" =
  let open Deferred.Or_error.Let_syntax in
  Deferred.Or_error.ok_exn
  @@ let%bind server, where_to_connect = make_server () in
  let%bind connection =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect)
  in
  let client = make_client () in
  let%bind response = Polling_state_rpc.Client.dispatch client connection "abc" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 32)))))
    ((foo abc) (bar 32)) |}];
  let%bind response = Polling_state_rpc.Client.dispatch client connection "def" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev (((foo abc) (bar 32)))) (query def)
     (diff (Update ((Foo def) (Bar 26)))))
    ((foo def) (bar 26)) |}];
  let%bind response = Polling_state_rpc.Client.redispatch client connection in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev (((foo def) (bar 26)))) (query def) (diff (Update ((Bar 27)))))
    ((foo def) (bar 27)) |}];
  let%bind () = Deferred.ok (Async.Tcp.Server.close server) in
  let%bind () = Deferred.ok (Async.Tcp.Server.close_finished server) in
  Deferred.ok (Async.Scheduler.yield_until_no_jobs_remain ())
;;

let%expect_test "initial-query" =
  let open Deferred.Or_error.Let_syntax in
  Deferred.Or_error.ok_exn
  @@ let%bind server, where_to_connect = make_server () in
  let%bind connection =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect)
  in
  let client = make_client ~initial_query:"abc" () in
  let%bind response = Polling_state_rpc.Client.redispatch client connection in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 32)))))
    ((foo abc) (bar 32)) |}];
  let%bind () = Deferred.ok (Async.Tcp.Server.close server) in
  let%bind () = Deferred.ok (Async.Tcp.Server.close_finished server) in
  Deferred.ok (Async.Scheduler.yield_until_no_jobs_remain ())
;;

let%expect_test "simulate server restarting " =
  let open Deferred.Or_error.Let_syntax in
  (* use two servers to simulate the server restarting *)
  Deferred.Or_error.ok_exn
  @@ let%bind _server1, where_to_connect1 = make_server () in
  let%bind _server2, where_to_connect2 = make_server () in
  let%bind connection1 =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect1)
  in
  let%bind connection2 =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect2)
  in
  let client = make_client () in
  (* using the first server *)
  let%bind response = Polling_state_rpc.Client.dispatch client connection1 "abc" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 32)))))
    ((foo abc) (bar 32)) |}];
  (* using the second server *)
  let%bind response = Polling_state_rpc.Client.redispatch client connection2 in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev (((foo abc) (bar 32)))) (query abc)
     (diff (Fresh ((foo abc) (bar 32)))))
    ((foo abc) (bar 32)) |}];
  (* notice that "bar" didn't change to 33 even though it otherwise would have been *)
  Deferred.ok (Async.Scheduler.yield_until_no_jobs_remain ())
;;

let%expect_test "multiple clients on the same connection" =
  let open Deferred.Or_error.Let_syntax in
  Deferred.Or_error.ok_exn
  @@ let%bind server, where_to_connect = make_server () in
  let%bind connection =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect)
  in
  let client1 = make_client () in
  let client2 = make_client () in
  let%bind response = Polling_state_rpc.Client.dispatch client1 connection "abc" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 32)))))
    ((foo abc) (bar 32)) |}];
  let%bind response = Polling_state_rpc.Client.dispatch client2 connection "abc" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 33)))))
    ((foo abc) (bar 33)) |}];
  let%bind response = Polling_state_rpc.Client.dispatch client1 connection "def" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev (((foo abc) (bar 32)))) (query def)
     (diff (Update ((Foo def) (Bar 27)))))
    ((foo def) (bar 27)) |}];
  let%bind response = Polling_state_rpc.Client.dispatch client2 connection "def" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev (((foo abc) (bar 33)))) (query def)
     (diff (Update ((Foo def) (Bar 28)))))
    ((foo def) (bar 28)) |}];
  let%bind () = Deferred.ok (Async.Tcp.Server.close server) in
  let%bind () = Deferred.ok (Async.Tcp.Server.close_finished server) in
  Deferred.ok (Async.Scheduler.yield_until_no_jobs_remain ())
;;

let%expect_test "simulate client disconnecting and reconnecting" =
  let open Deferred.Or_error.Let_syntax in
  Deferred.Or_error.ok_exn
  @@ let%bind server, where_to_connect = make_server () in
  let%bind connection =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect)
  in
  let client1 = make_client () in
  let%bind response = Polling_state_rpc.Client.dispatch client1 connection "abc" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 32)))))
    ((foo abc) (bar 32)) |}];
  let%bind () = Deferred.ok (Rpc.Connection.close connection) in
  let%bind () = Deferred.ok (Rpc.Connection.close_finished connection) in
  let%bind () = Deferred.ok (Async.Scheduler.yield_until_no_jobs_remain ()) in
  let%bind connection =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect)
  in
  let%bind response = Polling_state_rpc.Client.dispatch client1 connection "def" in
  print_s [%sexp (response : T.t)];
  (* notice that it's fresh now.  This implies that the user was removed from the cache. *)
  [%expect
    {|
    ((prev (((foo abc) (bar 32)))) (query def)
     (diff (Fresh ((foo def) (bar 26)))))
    ((foo def) (bar 26)) |}];
  let%bind response = Polling_state_rpc.Client.dispatch client1 connection "ghi" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev (((foo def) (bar 26)))) (query ghi)
     (diff (Update ((Foo ghi) (Bar 24)))))
    ((foo ghi) (bar 24)) |}];
  let%bind () = Deferred.ok (Async.Tcp.Server.close server) in
  let%bind () = Deferred.ok (Async.Tcp.Server.close_finished server) in
  Deferred.ok (Async.Scheduler.yield_until_no_jobs_remain ())
;;

let%expect_test "server throws exception" =
  let open Deferred.Or_error.Let_syntax in
  Deferred.Or_error.ok_exn
  @@ let%bind server, where_to_connect = make_server () in
  let%bind connection =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect)
  in
  let client1 = make_client () in
  let%bind response =
    Deferred.ok (Polling_state_rpc.Client.dispatch client1 connection "xyz")
  in
  let response_is_error = Or_error.is_error response in
  print_s [%message (response_is_error : bool)];
  [%expect {| (response_is_error true) |}];
  let%bind () = Deferred.ok (Async.Tcp.Server.close server) in
  let%bind () = Deferred.ok (Async.Tcp.Server.close_finished server) in
  Deferred.ok (Async.Scheduler.yield_until_no_jobs_remain ())
;;

let%expect_test "change query at a time that would otherwise cause blocking" =
  let open Deferred.Or_error.Let_syntax in
  Deferred.Or_error.ok_exn
  @@ let%bind server, where_to_connect =
       (* The server always blocks forever on the second (and third, and
          fourth, and ...) requests for the same query. *)
       make_server ~block:(fun () -> Deferred.never ()) ()
  in
  let%bind connection =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect)
  in
  let client = make_client () in
  let%bind response = Polling_state_rpc.Client.dispatch client connection "abc" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 32)))))
    ((foo abc) (bar 32)) |}];
  (* Dispatching again on the same client changes the query.  Thus, the fact
     that we get a response from this second dispatch shows that the server is
     going through the "first request" code path for the latest poll request. *)
  let%bind response = Polling_state_rpc.Client.dispatch client connection "def" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev (((foo abc) (bar 32)))) (query def)
     (diff (Update ((Foo def) (Bar 26)))))
    ((foo def) (bar 26)) |}];
  let%bind () = Deferred.ok (Async.Tcp.Server.close server) in
  let%bind () = Deferred.ok (Async.Tcp.Server.close_finished server) in
  Deferred.ok (Async.Scheduler.yield_until_no_jobs_remain ())
;;

let%expect_test "demonstrate that changing the query will cancel an ongoing request." =
  let open Deferred.Or_error.Let_syntax in
  let block = Bvar.create () in
  Deferred.Or_error.ok_exn
  @@ let%bind server, where_to_connect =
       make_server ~block:(fun _query -> Bvar.wait block) ()
  in
  let%bind connection =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect)
  in
  let client = make_client () in
  let%bind response = Polling_state_rpc.Client.dispatch client connection "abc" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 32)))))
    ((foo abc) (bar 32)) |}];
  (* Fire off another rpc with the same query to get the blocking behavior. *)
  let unbound_response = Polling_state_rpc.Client.dispatch client connection "abc" in
  let%bind response = Polling_state_rpc.Client.dispatch client connection "def" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev (((foo abc) (bar 32)))) (query def)
     (diff (Update ((Foo def) (Bar 26)))))
    ((foo def) (bar 26)) |}];
  let%bind.Deferred earlier_response = unbound_response in
  print_s [%sexp (earlier_response : T.t Or_error.t)];
  [%expect {| (Error "Request aborted") |}];
  Bvar.broadcast block ();
  let%bind () = Deferred.ok (Async.Tcp.Server.close server) in
  let%bind () = Deferred.ok (Async.Tcp.Server.close_finished server) in
  Deferred.ok (Async.Scheduler.yield_until_no_jobs_remain ())
;;

(* This test may seem silly at first glance, but there was previously a bug which caused a
   race condition in the following logic. Soak-testing this inline test failed previously,
   so we leave this test case for further soak-testing. *)

let%expect_test "redispatch does not race with dispatch if dispatch is called \
                 immediately afterwards"
  =
  let run_once () =
    let open Deferred.Or_error.Let_syntax in
    Deferred.Or_error.ok_exn
    @@ let%bind server, where_to_connect = make_server () in
    let%bind connection =
      Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect)
    in
    let client = make_client () in
    (* Fire off an rpc so that redispatch can be called *)
    let%bind response = Polling_state_rpc.Client.dispatch client connection "abc" in
    print_s [%sexp (response : T.t)];
    [%expect
      {|
      ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 32)))))
      ((foo abc) (bar 32)) |}];
    (* We fire off a redispatch and then a dispatch request. We expect that after these
       requests are completed, the query should be that of the dispatch operation: "def". *)
    let redispatch_response = Polling_state_rpc.Client.redispatch client connection in
    let dispatch_response =
      Polling_state_rpc.Client.dispatch client connection "def"
    in
    (* Both of the responses look as expected. *)
    let%bind redispatch_response = redispatch_response in
    print_s [%sexp (redispatch_response : T.t)];
    [%expect
      {|
      ((prev (((foo abc) (bar 32)))) (query abc) (diff (Update ((Bar 33)))))
      ((foo abc) (bar 33)) |}];
    let%bind dispatch_response = dispatch_response in
    print_s [%sexp (dispatch_response : T.t)];
    (* And doing one more redispatch shows us that the query was not modified by the
       redispatch query. *)
    [%expect
      {|
      ((prev (((foo abc) (bar 33)))) (query def)
       (diff (Update ((Foo def) (Bar 27)))))
      ((foo def) (bar 27)) |}];
    let%bind response = Polling_state_rpc.Client.redispatch client connection in
    print_s [%sexp (response : T.t)];
    [%expect
      {|
       ((prev (((foo def) (bar 27)))) (query def) (diff (Update ((Bar 28)))))
       ((foo def) (bar 28)) |}];
    let%bind () = Deferred.ok (Async.Tcp.Server.close server) in
    let%bind () = Deferred.ok (Async.Tcp.Server.close_finished server) in
    Deferred.ok (Async.Scheduler.yield_until_no_jobs_remain ())
  in
  (* The test is run 10 times to increase the chance of catching a future regression *)
  let i = ref 0 in
  while%bind return (!i < 10) do
    incr i;
    run_once ()
  done
;;

let%expect_test "[forget_on_server] does not clear the current query." =
  let open Deferred.Or_error.Let_syntax in
  Deferred.Or_error.ok_exn
  @@ let%bind server, where_to_connect = make_server () in
  let%bind connection =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect)
  in
  let client = make_client () in
  let%bind _response = Polling_state_rpc.Client.dispatch client connection "abc" in
  [%expect {| ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 32))))) |}];
  let%bind () = Polling_state_rpc.Client.forget_on_server client connection in
  let%bind.Deferred response = Polling_state_rpc.Client.redispatch client connection in
  print_s [%sexp (response : T.t Or_error.t)];
  [%expect
    {|
    ((prev (((foo abc) (bar 32)))) (query abc)
     (diff (Fresh ((foo abc) (bar 33)))))
    (Ok ((foo abc) (bar 33))) |}];
  let%bind () = Deferred.ok (Async.Tcp.Server.close server) in
  let%bind () = Deferred.ok (Async.Tcp.Server.close_finished server) in
  Deferred.ok (Async.Scheduler.yield_until_no_jobs_remain ())
;;

let%expect_test "[forget_on_server] cancels an ongoing request." =
  let open Deferred.Or_error.Let_syntax in
  let block = Bvar.create () in
  Deferred.Or_error.ok_exn
  @@ let%bind server, where_to_connect =
       make_server ~block:(fun _query -> Bvar.wait block) ()
  in
  let%bind connection =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect)
  in
  let client = make_client () in
  let%bind response = Polling_state_rpc.Client.dispatch client connection "abc" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 32)))))
    ((foo abc) (bar 32)) |}];
  (* Fire off another rpc with the same query to get the blocking behavior. *)
  let unbound_response = Polling_state_rpc.Client.dispatch client connection "abc" in
  let%bind () = Polling_state_rpc.Client.forget_on_server client connection in
  let%bind.Deferred earlier_response = unbound_response in
  print_s [%sexp (earlier_response : T.t Or_error.t)];
  [%expect {| (Error "Request aborted") |}];
  Bvar.broadcast block ();
  let%bind () = Deferred.ok (Async.Tcp.Server.close server) in
  let%bind () = Deferred.ok (Async.Tcp.Server.close_finished server) in
  Deferred.ok (Async.Scheduler.yield_until_no_jobs_remain ())
;;

let%expect_test "[forget_on_server] does not affect any other clients." =
  let open Deferred.Or_error.Let_syntax in
  Deferred.Or_error.ok_exn
  @@ let%bind server, where_to_connect = make_server () in
  let%bind connection =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect)
  in
  let client1 = make_client () in
  let client2 = make_client () in
  let%bind response = Polling_state_rpc.Client.dispatch client1 connection "abc" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 32)))))
    ((foo abc) (bar 32)) |}];
  let%bind response = Polling_state_rpc.Client.dispatch client2 connection "abc" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 33)))))
    ((foo abc) (bar 33)) |}];
  let%bind () = Polling_state_rpc.Client.forget_on_server client1 connection in
  let%bind response = Polling_state_rpc.Client.dispatch client1 connection "abc" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev (((foo abc) (bar 32)))) (query abc)
     (diff (Fresh ((foo abc) (bar 34)))))
    ((foo abc) (bar 34)) |}];
  let%bind response = Polling_state_rpc.Client.dispatch client2 connection "abc" in
  print_s [%sexp (response : T.t)];
  [%expect
    {|
    ((prev (((foo abc) (bar 33)))) (query abc) (diff (Update ((Bar 35)))))
    ((foo abc) (bar 35)) |}];
  let%bind () = Deferred.ok (Async.Tcp.Server.close server) in
  let%bind () = Deferred.ok (Async.Tcp.Server.close_finished server) in
  Deferred.ok (Async.Scheduler.yield_until_no_jobs_remain ())
;;

module Block : sig
  type t

  val create : unit -> t
  val block : t -> unit
  val unblock : t -> unit Deferred.t
  val unblocked : t -> unit Deferred.t
end = struct
  type t =
    { mutable is_blocked : bool
    ; unblocked_mvar : unit Mvar.Read_write.t
    }

  let create () = { is_blocked = false; unblocked_mvar = Mvar.create () }
  let block t = t.is_blocked <- true

  let unblock t =
    t.is_blocked <- false;
    Mvar.put t.unblocked_mvar ()
  ;;

  let unblocked t = if t.is_blocked then Mvar.take t.unblocked_mvar else return ()
end

type connection =
  { connection : Rpc.Connection.t
  ; reader_block : Block.t
  ; writer_block : Block.t
  }

let make_connection where_to_connect =
  let reader_block = Block.create () in
  let writer_block = Block.create () in
  let reader_r, reader_w = Pipe.create () in
  let writer_r, writer_w = Pipe.create () in
  let%bind reader_r = Reader.of_pipe (Info.of_string "reader_r") reader_r in
  let%bind writer_w, _ = Writer.of_pipe (Info.of_string "writer_w") writer_w in
  let make_transport fd ~max_message_size =
    let fd_reader = Reader.create fd in
    let fd_writer = Writer.create fd in
    don't_wait_for
      (Pipe.iter (Reader.pipe fd_reader) ~f:(fun message ->
         let%bind () = Block.unblocked reader_block in
         Pipe.write reader_w message));
    don't_wait_for
      (Pipe.iter writer_r ~f:(fun message ->
         let%bind () = Block.unblocked writer_block in
         Pipe.write (Writer.pipe fd_writer) message));
    Rpc.Transport.of_reader_writer ~max_message_size reader_r writer_w
  in
  let%map connection =
    Rpc.Connection.client ~make_transport where_to_connect
    |> Deferred.Or_error.of_exn_result
    >>| Or_error.ok_exn
  in
  { connection; writer_block; reader_block }
;;

let%expect_test "demonstrate that changing the query will abort any un-dispatched RPCs." =
  let bvar = Bvar.create () in
  let server_received_request = Mvar.create () in
  let%bind server, where_to_connect =
    make_server ~server_received_request ~block:(fun _query -> Bvar.wait bvar) ()
    >>| Or_error.ok_exn
  in
  let%bind { connection; writer_block; _ } = make_connection where_to_connect in
  let client = make_client () in
  let%bind response = Polling_state_rpc.Client.dispatch client connection "abc" in
  print_s [%sexp (response : T.t Or_error.t)];
  [%expect
    {|
    ((prev ()) (query abc) (diff (Fresh ((foo abc) (bar 32)))))
    (Ok ((foo abc) (bar 32))) |}];
  (* Fire off another rpc with the same query to get the blocking behavior. *)
  let response1 = Polling_state_rpc.Client.dispatch client connection "abc" in
  let%bind () = Mvar.take server_received_request in
  let response2 = Polling_state_rpc.Client.dispatch client connection "abc" in
  Block.block writer_block;
  let response3 = Polling_state_rpc.Client.dispatch client connection "def" in
  Bvar.broadcast bvar ();
  [%expect {| |}];
  let%bind response1 = response1 in
  [%expect {| ((prev (((foo abc) (bar 32)))) (query abc) (diff (Update ((Bar 33))))) |}];
  print_s [%sexp (response1 : T.t Or_error.t)];
  [%expect {| (Ok ((foo abc) (bar 33))) |}];
  let%bind () = Block.unblock writer_block in
  let%bind response2 = response2 in
  print_s [%sexp (response2 : T.t Or_error.t)];
  [%expect {| (Error "Request aborted") |}];
  let%bind response3 = response3 in
  print_s [%sexp (response3 : T.t Or_error.t)];
  [%expect
    {|
    ((prev (((foo abc) (bar 33)))) (query def)
     (diff (Update ((Foo def) (Bar 27)))))
    (Ok ((foo def) (bar 27)))
     |}];
  let%bind () = Async.Tcp.Server.close server in
  let%bind () = Async.Tcp.Server.close_finished server in
  Async.Scheduler.yield_until_no_jobs_remain ()
;;

let%expect_test "demonstrate that an [rpc_error] triggers a bug message." =
  let%bind server, where_to_connect = make_server () >>| Or_error.ok_exn in
  let%bind connection =
    Deferred.Or_error.of_exn_result (Rpc.Connection.client where_to_connect)
    >>| Or_error.ok_exn
  in
  let client = make_incompatible_client () in
  let%bind response = Polling_state_rpc.Client.dispatch client connection "abc" in
  print_s [%sexp (response : T_incompatible.t Or_error.t)];
  [%expect
    {|
    (Error
     ((rpc_error
       (Bin_io_exn
        ((location "client-side rpc response un-bin-io'ing")
         (exn (Failure "message length (9) did not match expected length (8)")))))
      (connection_description ("Client connected via TCP" 127.0.0.1:PORT))
      (rpc_name foo) (rpc_version 0))) |}];
  let%bind response1 = Polling_state_rpc.Client.dispatch client connection "abc" in
  print_s [%sexp (response1 : T_incompatible.t Or_error.t)];
  [%expect
    {|
    (lib/polling_state_rpc/src/polling_state_rpc.ml:LINE:COL
     "A polling state RPC client has requested a fresh response, but the server expected it to have the seqnum of the latest diffs. The server will send a fresh response as requested. This likely means that the client had trouble receiving the last RPC response."
     (rpc_name    foo)
     (rpc_version 0))
    (Error
     ((rpc_error
       (Bin_io_exn
        ((location "client-side rpc response un-bin-io'ing")
         (exn (Failure "message length (9) did not match expected length (8)")))))
      (connection_description ("Client connected via TCP" 127.0.0.1:PORT))
      (rpc_name foo) (rpc_version 0))) |}];
  let%bind response2 = Polling_state_rpc.Client.dispatch client connection "def" in
  print_s [%sexp (response2 : T_incompatible.t Or_error.t)];
  [%expect
    {|
    (lib/polling_state_rpc/src/polling_state_rpc.ml:LINE:COL
     "A polling state RPC client has requested a fresh response, but the server expected it to have the seqnum of the latest diffs. The server will send a fresh response as requested. This likely means that the client had trouble receiving the last RPC response."
     (rpc_name    foo)
     (rpc_version 0))
    (Error
     ((rpc_error
       (Bin_io_exn
        ((location "client-side rpc response un-bin-io'ing")
         (exn (Failure "message length (9) did not match expected length (8)")))))
      (connection_description ("Client connected via TCP" 127.0.0.1:PORT))
      (rpc_name foo) (rpc_version 0))) |}];
  let%bind () = Async.Tcp.Server.close server in
  let%bind () = Async.Tcp.Server.close_finished server in
  Async.Scheduler.yield_until_no_jobs_remain ()
;;
