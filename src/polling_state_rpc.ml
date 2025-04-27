open! Core
open! Async_kernel
open! Async_rpc_kernel

module Seqnum = struct
  include Unique_id.Int ()

  let forget = of_int_exn (-1)
end

module Query_dispatch_id = Unique_id.Int ()

(* A Client_id.t is unique per Client.t for a given connection.
   They are not guaranteed to be unique between connections. We need this
   because a user could have multiple Client.t for the same Rpc. *)
module Client_id = Unique_id.Int ()

module Cache : sig
  type ('a, 'client_state) t
  type ('a, 'client_state) per_client

  val create : on_client_forgotten:('client_state -> unit) -> ('a, 'client_state) t

  val find
    :  ('a, 'client_state) t
    -> connection_state:'connection_state
    -> connection:Rpc.Connection.t
    -> client_id:Client_id.t
    -> create_client_state:('connection_state -> 'client_state)
    -> ('a, 'client_state) per_client

  val remove_and_trigger_cancel
    :  ('a, 'client_state) t
    -> connection:Rpc.Connection.t
    -> client_id:Client_id.t
    -> unit

  val last_response : ('a, 'client_state) per_client -> ('a * Seqnum.t) option
  val set : ('a, 'client_state) per_client -> 'a -> Seqnum.t
  val wait_for_cancel : ('a, 'client_state) per_client -> unit Deferred.t
  val trigger_cancel : ('a, 'client_state) per_client -> unit
  val client_state : ('a, 'client_state) per_client -> 'client_state
end = struct
  type ('a, 'client_state) per_client =
    { mutable last_response : ('a * Seqnum.t) option
    ; mutable cancel : unit Ivar.t
    ; client_state : 'client_state
    }

  type ('a, 'client_state) per_connection =
    ('a, 'client_state) per_client Client_id.Table.t

  type ('a, 'client_state) t =
    { connections : (Rpc.Connection.t * ('a, 'client_state) per_connection) Bag.t
    ; on_client_forgotten : 'client_state -> unit
    }

  let create ~on_client_forgotten = { connections = Bag.create (); on_client_forgotten }

  let find_by_connection t ~connection =
    Bag.find t ~f:(fun (conn, _) -> phys_equal connection conn) |> Option.map ~f:snd
  ;;

  let find t ~connection_state ~connection ~client_id ~create_client_state =
    let per_connection =
      match find_by_connection t.connections ~connection with
      | Some per_connection -> per_connection
      | None ->
        let result = Client_id.Table.create () in
        let elt = Bag.add t.connections (connection, result) in
        Deferred.upon (Rpc.Connection.close_finished connection) (fun () ->
          Hashtbl.iter result ~f:(fun { client_state; _ } ->
            t.on_client_forgotten client_state);
          Bag.remove t.connections elt);
        result
    in
    Hashtbl.find_or_add per_connection client_id ~default:(fun () ->
      { last_response = None
      ; cancel = Ivar.create ()
      ; client_state = create_client_state connection_state
      })
  ;;

  let remove_and_trigger_cancel t ~connection ~client_id =
    Option.iter (find_by_connection t.connections ~connection) ~f:(fun per_connection ->
      Option.iter (Hashtbl.find_and_remove per_connection client_id) ~f:(fun per_client ->
        t.on_client_forgotten per_client.client_state;
        Ivar.fill_exn per_client.cancel ()))
  ;;

  let last_response per_client = per_client.last_response

  let set per_client data =
    let seqnum = Seqnum.create () in
    per_client.last_response <- Some (data, seqnum);
    seqnum
  ;;

  let wait_for_cancel per_client = Ivar.read per_client.cancel

  let trigger_cancel per_client =
    Ivar.fill_exn per_client.cancel ();
    per_client.cancel <- Ivar.create ()
  ;;

  let client_state per_client = per_client.client_state
end

module Request = struct
  (* We maintain two variations of the request type so that we can change one
     of them while keeping the other fixed. The stable type should be used only
     as an intermediate step of (de)serialization.

     Changing the unstable type requires figuring out how to encode/decode any
     additional data into/out-of the old stable type using the [unstable_of_stable]
     and [stable_of_unstable]. *)

  module Unstable = struct
    type 'query t =
      | Query of
          { last_seqnum : Seqnum.t option
          ; query : 'query
          ; client_id : Client_id.t
          }
      | Cancel_ongoing of Client_id.t
      | Forget_client of
          { query : 'query
          ; client_id : Client_id.t
          }
  end

  module Stable = struct
    type 'query t =
      | Query of
          { last_seqnum : Seqnum.t option
          ; query : 'query
          ; client_id : Client_id.t
          }
      | Cancel_ongoing of Client_id.t
    [@@deriving bin_io]

    let%expect_test _ =
      print_endline [%bin_digest: unit t];
      [%expect {| 4eb554fadd7eded37e4da89efd208c52 |}]
    ;;
  end

  let unstable_of_stable : 'query Stable.t -> 'query Unstable.t = function
    | Query { last_seqnum = Some last_seqnum; query; client_id }
      when [%equal: Seqnum.t] last_seqnum Seqnum.forget ->
      Forget_client { query; client_id }
    | Query { last_seqnum; query; client_id } -> Query { last_seqnum; query; client_id }
    | Cancel_ongoing client_id -> Cancel_ongoing client_id
  ;;

  let stable_of_unstable : 'query Unstable.t -> 'query Stable.t = function
    | Query { last_seqnum; query; client_id } -> Query { last_seqnum; query; client_id }
    | Cancel_ongoing client_id -> Cancel_ongoing client_id
    | Forget_client { query; client_id } ->
      Query { last_seqnum = Some Seqnum.forget; query; client_id }
  ;;
end

module Response = struct
  type ('response, 'update) t =
    | Fresh of 'response
    | Update of 'update
  [@@deriving bin_io, sexp_of]

  let%expect_test _ =
    print_endline [%bin_digest: (int, string) t];
    [%expect {| 13ef8c5223a0ea284c72512be32e5c09 |}]
  ;;

  type ('response, 'update) pair =
    | Response of
        { new_seqnum : Seqnum.t
        ; response : ('response, 'update) t
        }
    | Cancellation_successful
  [@@deriving bin_io]

  let%expect_test _ =
    print_endline [%bin_digest: (int, string) pair];
    [%expect {| 8bc63a85561d87b693d15e78c64e1008 |}]
  ;;
end

module type Response = sig
  (** This is a subset of the diffable signature (without [to_diffs] and [of_diffs]) and
      with the diffable type also supporting bin_io *)

  type t [@@deriving bin_io]

  module Update : sig
    type t [@@deriving bin_io, sexp_of]
  end

  val diffs : from:t -> to_:t -> Update.t
  val update : t -> Update.t -> t
end

type ('query, 'response) t =
  | T :
      { response_module :
          (module Response with type t = 'response and type Update.t = 'diff)
      ; query_equal : 'query -> 'query -> bool
      ; underlying_rpc :
          ('query Request.Stable.t, ('response, 'diff) Response.pair) Rpc.Rpc.t
      }
      -> ('query, 'response) t

let name (T { underlying_rpc; _ }) = Rpc.Rpc.name underlying_rpc
let version (T { underlying_rpc; _ }) = Rpc.Rpc.version underlying_rpc
let babel_generic_rpc (T { underlying_rpc; _ }) = Babel.Generic_rpc.Rpc underlying_rpc

let create
  (type a)
  ~name
  ~version
  ~query_equal
  ~bin_query
  (module M : Response with type t = a)
  =
  let bin_response = Response.bin_pair M.bin_t M.Update.bin_t in
  let bin_query = Request.Stable.bin_t bin_query in
  T
    { query_equal
    ; response_module = (module M)
    ; underlying_rpc =
        Rpc.Rpc.create
          ~name
          ~version
          ~bin_query
          ~bin_response
          ~include_in_error_count:Only_on_exn
    }
;;

let implement_with_client_state
  (type response)
  ~(here : [%call_pos])
  ~on_client_and_server_out_of_sync
  ~create_client_state
  ?(on_client_forgotten = ignore)
  ?for_first_request
  t
  f
  =
  (* make a new function to introduce a locally abstract type for diff *)
  let do_implement
    : type diff.
      response_module:(module Response with type t = response and type Update.t = diff)
      -> underlying_rpc:(_, (response, diff) Response.pair) Rpc.Rpc.t
      -> _
    =
    fun ~response_module ~underlying_rpc ~query_equal ->
    let module M = (val response_module) in
    let for_first_request = Option.value for_first_request ~default:f in
    let init connection_state client_state query =
      let%map response = for_first_request connection_state client_state query in
      response, response
    in
    let updates connection_state client_state ~prev:(prev_query, prev_response) query =
      let f = if query_equal prev_query query then f else for_first_request in
      let%map new_ = f connection_state client_state query in
      let diff = M.diffs ~from:prev_response ~to_:new_ in
      Response.Update diff, new_
    in
    let cache = Cache.create ~on_client_forgotten in
    Rpc.Rpc.implement ~here underlying_rpc (fun (connection_state, connection) request ->
      match Request.unstable_of_stable request with
      | Cancel_ongoing client_id ->
        let per_client =
          Cache.find cache ~connection_state ~connection ~client_id ~create_client_state
        in
        Cache.trigger_cancel per_client;
        return Response.Cancellation_successful
      | Forget_client { query = _; client_id } ->
        Cache.remove_and_trigger_cancel cache ~connection ~client_id;
        return Response.Cancellation_successful
      | Query { last_seqnum; query; client_id } ->
        let per_client =
          Cache.find cache ~connection_state ~connection ~client_id ~create_client_state
        in
        let prev =
          match Cache.last_response per_client, last_seqnum with
          | Some (prev, prev_seqnum), Some last_seqnum ->
            (match Seqnum.equal prev_seqnum last_seqnum with
             | true -> Some prev
             | false ->
               let rpc_name = Rpc.Rpc.name underlying_rpc in
               let rpc_version = Rpc.Rpc.version underlying_rpc in
               on_client_and_server_out_of_sync
                 [%message
                   [%here]
                     "A polling state RPC client has requested diffs from a seqnum that \
                      the server does not have, so the server is sending a fresh \
                      response instead. This likely means that the client had trouble \
                      receiving the last RPC response."
                     (rpc_name : string)
                     (rpc_version : int)];
               None)
          | None, Some _ | None, None -> None
          | Some _, None ->
            let rpc_name = Rpc.Rpc.name underlying_rpc in
            let rpc_version = Rpc.Rpc.version underlying_rpc in
            on_client_and_server_out_of_sync
              [%message
                [%here]
                  "A polling state RPC client has requested a fresh response, but the \
                   server expected it to have the seqnum of the latest diffs. The server \
                   will send a fresh response as requested. This likely means that the \
                   client had trouble receiving the last RPC response."
                  (rpc_name : string)
                  (rpc_version : int)];
            None
        in
        let response =
          let client_state = Cache.client_state per_client in
          match prev with
          | Some prev ->
            let%map response, userdata =
              updates connection_state client_state ~prev query
            in
            response, userdata
          | None ->
            let%map response, userdata = init connection_state client_state query in
            Response.Fresh response, userdata
        in
        let response_or_cancelled =
          choose
            [ choice response (fun r -> `Response r)
            ; choice (Cache.wait_for_cancel per_client) (fun () -> `Cancelled)
            ]
        in
        (match%map response_or_cancelled with
         | `Response (response, userdata) ->
           let new_seqnum = Cache.set per_client (query, userdata) in
           Cache.trigger_cancel per_client;
           Response.Response { new_seqnum; response }
         | `Cancelled -> Response.Cancellation_successful))
  in
  let (T { query_equal; response_module; underlying_rpc }) = t in
  do_implement ~response_module ~underlying_rpc ~query_equal
;;

let implement
  ~(here : [%call_pos])
  ~on_client_and_server_out_of_sync
  ?for_first_request
  t
  f
  =
  let for_first_request =
    Option.map for_first_request ~f:(fun f connection_state _client_state query ->
      f connection_state query)
  in
  let f connection_state _client_state query = f connection_state query in
  implement_with_client_state
    ~here
    ~on_client_and_server_out_of_sync
    ~create_client_state:(fun _ -> ())
    ?for_first_request
    t
    f
;;

let implement_via_bus
  ~(here : [%call_pos])
  ~on_client_and_server_out_of_sync
  ~create_client_state
  ?on_client_forgotten
  rpc
  f
  =
  implement_with_client_state
    ~here
    ~on_client_and_server_out_of_sync
    ~create_client_state:(fun connection_state ->
      Bus_state.create (), create_client_state connection_state)
    ~on_client_forgotten:(fun (bus_state, client_state) ->
      Bus_state.unsubscribe bus_state;
      Option.iter on_client_forgotten ~f:(fun on_client_forgotten ->
        on_client_forgotten client_state))
    rpc
    ~for_first_request:(fun connection_state (bus_state, client_state) query ->
      (* Set up the bus subscription to the new query. *)
      let%bind.Eager_deferred bus = f connection_state client_state query in
      (* Subscribe to the new bus, unsubscribing to the previous bus if necessary.
         It is good to subscribe to the bus as immediately after receiving the bus
         via Eager_deferred, such that the publisher to the bus has an accurate
         count of subscribers from [Bus.num_subscribers]. *)
      Bus_state.subscribe bus_state bus;
      (* Wait for the bus to publish something to the [Bus_state.t] so we can return it as
         the response. *)
      Bus_state.take bus_state)
    (fun _connection_state (bus_state, _client_state) _query ->
      (* For all polls except the first one for each query, we can simply wait
          for the current bus to publish a new response. We ignore the query,
          because we know that the bus subscription only ever corresponds to
          the current query. *)
      Bus_state.take bus_state)
;;

let implement_via_bus'
  ~(here : [%call_pos])
  ~on_client_and_server_out_of_sync
  ~create_client_state
  ?on_client_forgotten
  rpc
  f
  =
  implement_via_bus
    ~here
    ~on_client_and_server_out_of_sync
    ~create_client_state
    ?on_client_forgotten
    rpc
    (fun connection_state client_state query ->
       f connection_state client_state query |> Deferred.return)
;;

module Client = struct
  type ('a, 'b) x = ('a, 'b) t

  type ('query, 'response, 'diff) unpacked =
    { mutable last_seqnum : Seqnum.t option
    ; mutable last_query : 'query option
    ; mutable last_query_dispatch_id : Query_dispatch_id.t
    ; mutable out : 'response option
    ; mutable sequencer : unit Sequencer.t
    ; cleaning_sequencer : unit Sequencer.t
    ; client_id : Client_id.t
    ; bus : ('query -> 'response -> unit) Bus.Read_write.t
    ; response_module :
        (module Response with type t = 'response and type Update.t = 'diff)
    ; query_equal : 'query -> 'query -> bool
    ; underlying_rpc :
        ('query Request.Stable.t, ('response, 'diff) Response.pair) Rpc.Rpc.t
    ; fold : 'response option -> 'query -> ('response, 'diff) Response.t -> 'response
    }

  type ('query, 'response) t =
    | T : ('query, 'response, 'diff) unpacked -> ('query, 'response) t

  let dispatch_underlying t connection request =
    Rpc.Rpc.dispatch t.underlying_rpc connection (Request.stable_of_unstable request)
  ;;

  let cancel_current_if_query_changed t q connection =
    (* use this cleaning_sequencer as a lock to prevent two subsequent queries
       from obliterating the same sequencer. *)
    Throttle.enqueue' t.cleaning_sequencer (fun () ->
      match t.last_query with
      (* If there was no previous query, we can keep the existing sequencer
         because its totally empty. *)
      | None -> Deferred.Or_error.ok_unit
      (* If the sequencer isn't running anything right now, then we can co-opt it
         for this query. *)
      | Some _ when Throttle.num_jobs_running t.sequencer = 0 -> Deferred.Or_error.ok_unit
      (* If the current query is the same as the last one, then we're fine
         because the sequencer is already running that kind of query. *)
      | Some q' when t.query_equal q q' -> Deferred.Or_error.ok_unit
      (* Otherwise, we need to cancel the current request, kill every task currently
         in the sequencer, and make a new sequencer for this query. *)
      | _ ->
        Throttle.kill t.sequencer;
        let%bind cancel_response =
          dispatch_underlying t connection (Cancel_ongoing t.client_id)
        in
        let%map () = Throttle.cleaned t.sequencer in
        t.sequencer <- Sequencer.create ~continue_on_error:true ();
        (match cancel_response with
         | Ok (Response _) ->
           [%message "BUG" [%here] "regular response caused by cancellation"]
           |> Or_error.error_s
         | Ok Cancellation_successful -> Ok ()
         | Error e -> Error e))
  ;;

  module Aborted_error : sig
    val the_one_and_only : Error.t
    val test : Error.t -> bool
  end = struct
    exception Aborted [@@deriving sexp_of]

    let the_one_and_only = Error.of_exn Aborted
    let test = phys_equal the_one_and_only
  end

  let dispatch'
    (type query response diff)
    ?(sexp_of_response : (response -> Sexp.t) option)
    (t : (query, response, diff) unpacked)
    connection
    query
    =
    t.last_query <- Some query;
    let%bind response =
      let last_seqnum = t.last_seqnum in
      let client_id = t.client_id in
      dispatch_underlying t connection (Query { query; last_seqnum; client_id })
    in
    match response with
    | Ok (Response { response; new_seqnum }) ->
      let new_out = t.fold t.out query response in
      t.last_seqnum <- Some new_seqnum;
      t.out <- Some new_out;
      Bus.write2 t.bus query new_out;
      let underlying_diff =
        lazy
          (let module User_response :
             Response with type t = response and type Update.t = diff =
             (val t.response_module)
           in
          let module User_response = struct
            include User_response

            let sexp_of_t = Option.value ~default:sexp_of_opaque sexp_of_response
          end
          in
          [%sexp_of: (User_response.t, User_response.Update.t) Response.t] response)
      in
      return (Ok (new_out, underlying_diff))
    | Ok Cancellation_successful -> return (Error Aborted_error.the_one_and_only)
    | Error e -> return (Error e)
  ;;

  (* These are errors defined by sequencer; squish them into Or_error.t *)
  let collapse_sequencer_error = function
    | `Ok result_or_error -> result_or_error
    | `Aborted -> Error (Error.of_string "Request aborted")
    | `Raised exn -> Error (Error.of_exn exn)
  ;;

  let fix_sequencer_error = function
    | `Ok (Error e) when Aborted_error.test e -> `Aborted
    | other -> other
  ;;

  (* Use a sequencer to ensure that there aren't any sequential outgoing requests *)
  let dispatch_with_underlying_diff ?sexp_of_response (T t) connection query =
    let query_dispatch_id = Query_dispatch_id.create () in
    t.last_query_dispatch_id <- query_dispatch_id;
    match%bind cancel_current_if_query_changed t query connection with
    | (`Aborted | `Raised _ | `Ok (Error _)) as result -> return result
    | `Ok (Ok ()) ->
      if not (Query_dispatch_id.equal t.last_query_dispatch_id query_dispatch_id)
      then return `Aborted
      else (
        let%map result =
          Throttle.enqueue' t.sequencer (fun () ->
            dispatch' ?sexp_of_response t connection query)
        in
        fix_sequencer_error result)
  ;;

  let dispatch rpc connection query =
    let%map.Deferred.Or_error response, (_ : Sexp.t lazy_t) =
      dispatch_with_underlying_diff rpc connection query >>| collapse_sequencer_error
    in
    response
  ;;

  let redispatch (T t) connection =
    let%map result =
      Throttle.enqueue' t.sequencer (fun () ->
        match t.last_query with
        | Some q ->
          let%map.Deferred.Or_error response, (_ : Sexp.t lazy_t) =
            dispatch' t connection q
          in
          response
        | None ->
          "[redispatch] called before a query was set or a regular dispatch had completed"
          |> Error.of_string
          |> Error
          |> Deferred.return)
    in
    collapse_sequencer_error (fix_sequencer_error result)
  ;;

  let forget_on_server (T t) connection =
    t.last_query_dispatch_id <- Query_dispatch_id.create ();
    match t.last_query with
    (* If there was no previous query, then the server has nothing to forget. *)
    | None -> Deferred.Or_error.ok_unit
    | Some query ->
      Throttle.enqueue' t.cleaning_sequencer (fun () ->
        Throttle.kill t.sequencer;
        let%bind forget_response =
          dispatch_underlying
            t
            connection
            (Forget_client { query; client_id = t.client_id })
        in
        let%map () = Throttle.cleaned t.sequencer in
        t.sequencer <- Sequencer.create ~continue_on_error:true ();
        match forget_response with
        | Ok (Response _) ->
          (* It is possible that new clients of old servers will get
             spammed with this message if [forget_on_server] is called a lot. However,
             this sounds like an unlikely case to me, since old servers probably won't
             exist for much longer; in addition, spamming this message is not terrible. *)
          Or_error.error_s
            [%message
              "BUG"
                [%here]
                {|Regular response caused by forget request. This can also happen if the server is old and does not support forget requests, in which case this is not a bug.|}]
        | Ok Cancellation_successful -> Ok ()
        | Error e -> Error e)
      >>| collapse_sequencer_error
  ;;

  let query (T { last_query; _ }) = last_query
  let bus (T { bus; _ }) = Bus.read_only bus

  let create (type query response) ?initial_query (t : (query, response) x) =
    let (T { query_equal; underlying_rpc; response_module } : _ x) = t in
    let module M = (val response_module) in
    let f prev _query = function
      | Response.Fresh r -> r
      | Update diffs ->
        (match prev with
         | None ->
           raise_s
             [%message
               "BUG" [%here] "received an update without receiving any previous values"]
         | Some prev -> M.update prev diffs)
    in
    let bus =
      Bus.create_exn
        Bus.Callback_arity.Arity2
        ~on_callback_raise:(fun error ->
          let tag = "exception thrown from inside of polling-state-rpc bus handler" in
          error |> Error.tag ~tag |> [%sexp_of: Error.t] |> eprint_s)
        ~on_subscription_after_first_write:Allow_and_send_last_value
    in
    let sequencer = Sequencer.create ~continue_on_error:true () in
    let cleaning_sequencer = Sequencer.create ~continue_on_error:true () in
    T
      { bus
      ; sequencer
      ; cleaning_sequencer
      ; last_seqnum = None
      ; last_query = initial_query
      ; last_query_dispatch_id = Query_dispatch_id.create ()
      ; out = None
      ; response_module
      ; query_equal
      ; underlying_rpc
      ; fold = f
      ; client_id = Client_id.create ()
      }
  ;;

  module For_introspection = struct
    let dispatch_with_underlying_diff = dispatch_with_underlying_diff
  end
end

module Private_for_testing = struct
  module Response' = struct
    type 'response t =
      | Fresh : 'response -> 'response t
      | Update :
          { t : 'update
          ; sexp_of : 'update -> Sexp.t
          ; bin_size_t : 'update -> int
          }
          -> 'response t

    let sexp_of_t sexp_of_a = function
      | Fresh a -> [%sexp Fresh (sexp_of_a a : Sexp.t)]
      | Update { t; sexp_of; _ } -> [%sexp Update (sexp_of t : Sexp.t)]
    ;;

    let base_bin_size_t = [%bin_size: [ `Fresh | `Update ]]

    let bin_size_t bin_size_a = function
      | Fresh a -> base_bin_size_t `Fresh + bin_size_a a
      | Update { t; bin_size_t; _ } -> base_bin_size_t `Update + bin_size_t t
    ;;

    let is_fresh = function
      | Fresh _ -> true
      | Update _ -> false
    ;;
  end

  let create_client
    (type query response)
    ?initial_query
    (t : (query, response) t)
    ~introspect
    =
    (* Make a new function to introduce a locally abstract type for [diff] *)
    let make_fold
      : type diff.
        fold:(response option -> query -> (response, diff) Response.t -> response)
        -> response_module:
             (module Response with type t = response and type Update.t = diff)
        -> (response option -> query -> (response, diff) Response.t -> response)
      =
      fun ~fold ~response_module ->
      let module M = (val response_module) in
      let fold prev query (resp : (response, diff) Response.t) =
        let resp' =
          match resp with
          | Response.Fresh r -> Response'.Fresh r
          | Update diffs ->
            Update
              { t = diffs
              ; sexp_of = [%sexp_of: M.Update.t]
              ; bin_size_t = [%bin_size: M.Update.t]
              }
        in
        introspect prev query resp';
        fold prev query resp
      in
      fold
    in
    let (T client) = Client.create ?initial_query t in
    let new_fold = make_fold ~fold:client.fold ~response_module:client.response_module in
    Client.T { client with fold = new_fold }
  ;;

  module Response = Response'
end
