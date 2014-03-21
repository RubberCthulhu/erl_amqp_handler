%%%-------------------------------------------------------------------
%%% @author Danil Onishchenko
%%% @copyright (C) 2014, Danil Onishchenko
%%% @doc
%%%
%%% @end
%%% Created : 21 Mar 2014 by Danil Onishchenko
%%%-------------------------------------------------------------------
-module(amqp_handler_manager).

-behaviour(gen_server).

%% API
-export([start_link/7]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
	  sup,
	  conn_attrs,
	  conn,
	  conn_monitor,
	  exchange_declare,
	  routing_key,
	  listeners_number,
	  cb_module,
	  cb_args
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(SupPid, ConnAttrs, ExchangeDeclare, RoutingKey, N, CbModule, CbArgs) ->
    gen_server:start_link(?MODULE, [SupPid, ConnAttrs, ExchangeDeclare, RoutingKey, N, CbModule, CbArgs], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([SupPid, ConnAttrs, ExchangeDeclare, RoutingKey, N, CbModule, CbArgs]) ->
    {ok, Conn} = amqp_connection:start(ConnAttrs),
    ConnMonitor = monitor(process, Conn),

    State = #state{
	       sup = SupPid,
	       conn_attrs = ConnAttrs,
	       conn = Conn,
	       conn_monitor = ConnMonitor,
	       exchange_declare = ExchangeDeclare,
	       routing_key = RoutingKey,
	       listeners_number = N,
	       cb_module = CbModule,
	       cb_args = CbArgs
	      },

    self() ! start,

    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(start, State) ->
    #state{
       sup = SupPid,
       cb_module = CbModule,
       cb_args = CbArgs,
       conn = Conn,
       exchange_declare = ExchangeDeclare,
       routing_key = RoutingKey,
       listeners_number = N
      } = State,
    {ok, WorkerSup} = amqp_handler:start_worker_sup(SupPid, CbModule, CbArgs),
    {ok, ListenerSup} = amqp_handler:start_listener_sup(SupPid, Conn, ExchangeDeclare, RoutingKey, N, WorkerSup),

    link(WorkerSup),
    link(ListenerSup),

    ok;

handle_info({'DOWN', ConnMonitor, process, Conn, _Info},
	    #state{conn_monitor = ConnMonitor, conn = Conn} = State) ->
    {stop, {error, amqp_connection_process_shutdown}, State};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
