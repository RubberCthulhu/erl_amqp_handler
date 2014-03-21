%%%-------------------------------------------------------------------
%%% @author Danil Onishchenko <alevandal@kernelpanic>
%%% @copyright (C) 2014, Danil Onishchenko
%%% @doc
%%%
%%% @end
%%% Created : 20 Mar 2014 by Danil Onishchenko <alevandal@kernelpanic>
%%%-------------------------------------------------------------------
-module(amqp_handler_worker_sup).

-behaviour(supervisor).

%% API
-export([start_link/2, start_child/4]).

%% Supervisor callbacks
-export([init/1]).

%%-define(SERVER, ?MODULE).

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
start_link(CbModule, CbArgs) ->
    supervisor:start_link(?MODULE, [CbModule, CbArgs]).

start_child(Pid, Chan, Deliver, Msg) ->
    supervisor:start_child(Pid, [Chan, Deliver, Msg]).

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
init([CbModule, Args]) ->
    RestartStrategy = simple_one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = temporary,
    Shutdown = 2000,
    Type = worker,

    Worker = {amqp_handler_worker, {amqp_handler_worker, start_link, [CbModule, Args]},
	      Restart, Shutdown, Type, [amqp_handler_worker]},

    {ok, {SupFlags, [Worker]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
