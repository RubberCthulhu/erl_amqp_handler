%%%-------------------------------------------------------------------
%%% @author Danil Onishchenko
%%% @copyright (C) 2014, Danil Onishchenko
%%% @doc
%%%
%%% @end
%%% Created : 21 Mar 2014 by Danil Onishchenko
%%%-------------------------------------------------------------------
-module(amqp_handler_listener_sup).

-behaviour(supervisor).

%% API
-export([start_link/5]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Conn, ExchangeDeclare, RoutingKey, N, WorkerSup) ->
    supervisor:start_link(?MODULE, [Conn, ExchangeDeclare, RoutingKey, N, WorkerSup]).

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
init([Conn, ExchangeDeclare, RoutingKey, N, WorkerSup]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = permanent,
    Shutdown = 2000,
    Type = worker,

    Listeners = [{{amqp_handler_listener, make_ref()},
		  {amqp_handler_listener, start_link, [Conn, ExchangeDeclare, RoutingKey, WorkerSup]},
		  Restart, Shutdown, Type, [amqp_handler_listener]}
		 || _ <- lists:seq(1, N)],

    {ok, {SupFlags, Listeners}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
