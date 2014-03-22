
-module(amqp_handler).

-behaviour(supervisor).

%% API
-export([start/0, start/1, stop/0]).
-export([start_handler/7, stop_handler/1]).
-export([start_link/6, start_worker_sup/3, start_listener_sup/6]).

%% Callback
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

start() ->
    application:start(amqp_handler).

start(Type) ->
    application:start(amqp_handler, Type).

stop() ->
    application:stop(amqp_handler).

start_handler(Id, ConnAttrs, ExchangeDeclare, RoutingKey, N, CbModule, CbArgs) ->
    Spec = {Id,
	    {amqp_handler, start_link, [ConnAttrs, ExchangeDeclare, RoutingKey, N, CbModule, CbArgs]},
	    permanent, 2000, supervisor, [amqp_handler]},
    supervisor:start_child(amqp_handler_sup, Spec).

stop_handler(Id) ->
    supervisor:terminate_child(amqp_handler_sup, Id),
    supervisor:delete_child(amqp_handler_sup, Id).

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(ConnAttrs, ExchangeDeclare, RoutingKey, N, CbModule, CbArgs) ->
    supervisor:start_link(?MODULE, [ConnAttrs, ExchangeDeclare, RoutingKey, N, CbModule, CbArgs]).

start_worker_sup(Pid, CbModule, CbArgs) ->
    Spec = {amqp_handler_worker_sup, {amqp_handler_worker_sup, start_link, [CbModule, CbArgs]},
	    permanent, 2000, supervisor, [amqp_handler_worker_sup]},
    Children = [Id || {Id, _, _, _} <- supervisor:which_children(Pid)],
    case lists:member(amqp_handler_worker_sup, Children) of
	true ->
	    {error, worker_sup_already_started};
	false ->
	    supervisor:start_child(Pid, Spec)
    end.

start_listener_sup(Pid, Conn, ExchangeDeclare, RoutingKey, N, WorkerSup) ->
    Spec = {amqp_handler_listener_sup,
	    {amqp_handler_listener_sup, start_link, [Conn, ExchangeDeclare, RoutingKey, N, WorkerSup]},
	    permanent, 2000, supervisor, [amqp_handler_listener_sup]},
    Children = [Id || {Id, _, _, _} <- supervisor:which_children(Pid)],
    case lists:member(amqp_handler_listener_sup, Children) of
	true ->
	    {error, listener_sup_already_started};
	false ->
	    supervisor:start_child(Pid, Spec)
    end.

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([ConnParams, ExchangeDeclare, RoutingKey, N, CbModule, CbArgs]) ->
    RestartStrategy = one_for_one_all,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = permanent,
    Shutdown = 2000,
    Type = worker,

    Manager = {amqp_handler_manager,
	       {amqp_handler_manager, start_link, [self(), ConnParams, ExchangeDeclare, RoutingKey, N, CbModule, CbArgs]},
	       Restart, Shutdown, Type, [amqp_handler_manager]},

    {ok, {SupFlags, [Manager]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================



