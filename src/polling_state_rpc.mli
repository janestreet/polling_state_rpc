open! Core
open! Async_kernel
open! Async_rpc_kernel

module type Response = sig
  (** This is a subset of the diffable signature (without [to_diffs] and [of_diffs])
      and with the diffable type also supporting bin_io *)

  type t [@@deriving bin_io]

  module Update : sig
    type t [@@deriving bin_io, sexp_of]
  end

  val diffs : from:t -> to_:t -> Update.t
  val update : t -> Update.t -> t
end

(** The type representing the RPC itself.  Similar to Rpc.Rpc.t and its friends. *)
type ('query, 'response) t

val name : ('query, 'response) t -> string
val version : ('query, 'response) t -> int
val babel_generic_rpc : _ t -> Babel.Generic_rpc.t

val create
  :  name:string
  -> version:int
  -> query_equal:('query -> 'query -> bool)
  -> bin_query:'query Bin_prot.Type_class.t
  -> (module Response with type t = 'response)
  -> ('query, 'response) t

(** Handling the first request and handling the remaining requests is different because
    the first request you likely want to compute immediately, but for subsequent requests,
    it's more likely that you'll want to block until your data store changes and updates
    are made available. *)
val implement
  :  on_client_and_server_out_of_sync:(Sexp.t -> unit)
       (** [on_client_and_server_out_of_sync] gets called when the client asks for either diffs
      from a point different than the server expected or a fresh response when the server
      was expecting it to ask for diffs.

      This can happen when the response type's bin_shape changes, which means that the
      client will receive responses but won't be able to deserialize them. It can also
      happen when the client's heartbeat times out.


      In both of these cases the server behaves reasonably (it falls back to sending a
      fresh response), but passes a message to [on_client_and_server_out_of_sync] because
      the situation is undesirable, so server owners should be made aware so that it can
      be addressed.

      Some good values for this parameter include:
      - [Log.Global.info_s] - the mismatch is logged, but otherwise not handled.
      - [raise_s] - the server will get to skip sending the whole state across the wire
        and will instead merely send an error. This is often a good choice because usually
        if clients have trouble receiving one response, they continue to have trouble
        receiving more responses (for example, bin_shape errors don't go away naturally). *)
  -> ?for_first_request:('connection_state -> 'query -> 'response Deferred.t)
       (** When provided, [for_first_request] is called instead of the primary implementation function
      for the first_request of a particular query.  This is so that you can respond immediately to
      the first request, but block on subsequent requests until there's an update to send. *)
  -> ('query, 'response) t
  -> ('connection_state -> 'query -> 'response Deferred.t)
  -> ('connection_state * Rpc.Connection.t) Rpc.Implementation.t

(** Similar to [implement], but with support for per-client state. This allows you to
    support multiple clients, each with different server-side state, that share a single
    [Rpc.Connection.t].

    Note that, similar to the connection state, the client state cannot be shared between
    multiple connections - if a client reconnects after disconnecting, it will be given a
    new client state.

    [on_client_forgotten] is called when the client calls [Client.forget_on_server], or
    when the underlying connection is closed. *)
val implement_with_client_state
  :  on_client_and_server_out_of_sync:(Sexp.t -> unit)
  -> create_client_state:('connection_state -> 'client_state)
  -> ?on_client_forgotten:('client_state -> unit)
  -> ?for_first_request:
       ('connection_state -> 'client_state -> 'query -> 'response Deferred.t)
  -> ('query, 'response) t
  -> ('connection_state -> 'client_state -> 'query -> 'response Deferred.t)
  -> ('connection_state * Rpc.Connection.t) Rpc.Implementation.t

(** Like [implement], except the callback is invoked only once per query, and
    must return a bus instead of a single result. Each time a client polls, it
    will receive the newest response that it has not yet seen. If it has seen
    the newest response, then the RPC implementation will block until there is
    a newer response.

    This function immediately subscribes to the returned bus and cancels any
    previous subscriptions each time it invokes the callback. It is recommended
    that the bus use [Allow_and_send_last_value], so that each new query
    doesn't miss the first response for each query (or even raise, if
    [on_subscription_after_first_write] is set to [Raise]). *)
val implement_via_bus
  :  on_client_and_server_out_of_sync:(Sexp.t -> unit)
  -> create_client_state:('connection_state -> 'client_state)
  -> ?on_client_forgotten:('client_state -> unit)
  -> ('query, 'response) t
  -> ('connection_state
      -> 'client_state
      -> 'query
      -> ('response -> unit, [> read ]) Bus.t Deferred.t)
  -> ('connection_state * Rpc.Connection.t) Rpc.Implementation.t

(** Like [implement_via_bus], except that the callback does not return a deferred.

    [implement_via_bus'] will call [Bus.subscribe] immediately after the callback
    returns. When the client is no longer polling for the given query, this will
    call [Bus.unsubscribe'], meaning that [Bus.num_subscribers] will count the client
    as a subscriber iff the client is still polling for this query.
*)
val implement_via_bus'
  :  on_client_and_server_out_of_sync:(Sexp.t -> unit)
  -> create_client_state:('connection_state -> 'client_state)
  -> ?on_client_forgotten:('client_state -> unit)
  -> ('query, 'response) t
  -> ('connection_state
      -> 'client_state
      -> 'query
      -> ('response -> unit, [> read ]) Bus.t)
  -> ('connection_state * Rpc.Connection.t) Rpc.Implementation.t

module Client : sig
  type ('query, 'response) rpc := ('query, 'response) t

  (** A Client.t is the method by which polling-state-rpcs are dispatched to the server,
      and their results collected by the client.  Clients independently track responses
      and perform diff updates, so if you have two state-rpcs that share the same
      underlying rpc, you should make a new client for each one. *)
  type ('query, 'response) t

  val create : ?initial_query:'query -> ('query, 'response) rpc -> ('query, 'response) t

  (** Dispatch will call the rpc and return the corresponding response. If you're
      listening for responses via [bus], the response will also be communicated there. *)
  val dispatch
    :  ('query, 'response) t
    -> Rpc.Connection.t
    -> 'query
    -> 'response Deferred.Or_error.t

  (** Same as [dispatch] but reusing the previous query.  This function returns an Error
      if the query was not set with [dispatch] or [create ~initial_query] beforehand. *)
  val redispatch
    :  ('query, 'response) t
    -> Rpc.Connection.t
    -> 'response Deferred.Or_error.t

  (** Asks the server to forget any state related to the specified client. Use this
      function on clients that might not be used again, so that the server can free up
      memory. If the server side of the RPC was built using [implement_with_client_state]
      then a `on_client_forgotten` function (if provided) will be called.

      In addition, any queued and ongoing [dispatch]es will get cancelled.

      Calling [dispatch] after [forget_on_server] works just fine, but
      will require the server to send the entire response, rather than merely
      the diff from the previous response. In other words, clearing a client
      only affects speed/memory; it should have no effect on the results
      returned by subsequent calls to [dispatch] or [redispatch]. *)
  val forget_on_server
    :  ('query, 'response) t
    -> Rpc.Connection.t
    -> unit Deferred.Or_error.t

  (** Returns the most recent query. *)
  val query : ('query, _) t -> 'query option

  (** Receives a [bus] which forwards all the responses that come from this
      client alongside the query which requested them. *)
  val bus : ('query, 'response) t -> ('query -> 'response -> unit) Bus.Read_only.t
end

module Private_for_testing : sig
  module Response : sig
    type 'response t [@@deriving sexp_of]
  end

  val create_client
    :  ?initial_query:'query
    -> ('query, 'response) t
    -> introspect:('response option -> 'query -> 'response Response.t -> unit)
    -> ('query, 'response) Client.t
end
