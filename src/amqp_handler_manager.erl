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
-export([start_link/7, start_link/8, wait_for_ready/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

-define(SERVER, ?MODULE).

-record(state, {
	  sup :: pid(),
	  conn_attrs,
	  conn = undefined :: pid(),
	  conn_monitor = undefined :: reference(),
	  exchange_declare,
	  queue_declare,
	  routing_key :: binary(),
	  consumers_number :: non_neg_integer(),
	  cb_module,
	  cb_args,
	  state = init :: init | start | work,
	  waiters = [] :: [pid()],
	  keepalive = false :: boolean(),
	  connect_timeout = 5000 :: non_neg_integer(),
	  connect_limit = infinity :: infinity | non_neg_integer(),
	  connect_number = 0 :: non_neg_integer()
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
start_link(SupPid, ConnAttrs, ExchangeDeclare, RoutingKey, NumberOfConsumers, CbModule, CbArgs) ->
    gen_server:start_link(?MODULE, [SupPid, ConnAttrs, ExchangeDeclare, RoutingKey, NumberOfConsumers, CbModule, CbArgs, []], []).

start_link(SupPid, ConnAttrs, ExchangeDeclare, RoutingKey, NumberOfConsumers, CbModule, CbArgs, Opts) ->
    gen_server:start_link(?MODULE, [SupPid, ConnAttrs, ExchangeDeclare, RoutingKey, NumberOfConsumers, CbModule, CbArgs, Opts], []).

wait_for_ready(Pid, Timeout) ->
    gen_server:call(Pid, wait_for_ready, Timeout).

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
init([SupPid, ConnAttrs, ExchangeDeclare, RoutingKey, NumberOfConsumers, CbModule, CbArgs, Opts]) ->
    QueueDeclare = case proplists:lookup(queue_declare, Opts) of
		       {_, #'queue.declare'{} = Declare} ->
			   Declare;
		       none ->
			   #'queue.declare'{
			      auto_delete = true
			     }
		   end,
    State = #state{
	       sup = SupPid,
	       conn_attrs = ConnAttrs,
	       exchange_declare = ExchangeDeclare,
	       queue_declare = QueueDeclare,
	       routing_key = RoutingKey,
	       consumers_number = NumberOfConsumers,
	       cb_module = CbModule,
	       cb_args = CbArgs,
	       state = init,
	       keepalive = proplists:get_bool(keepalive, Opts),
	       connect_timeout = proplists:get_value(connect_timeout, Opts, 5000),
	       connect_limit = proplists:get_value(connect_limit, Opts, infinity)
	      },

    self() ! init,

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
handle_call(wait_for_ready, From, #state{state = start, waiters = Waiters} = State) ->
    {noreply, State#state{waiters = [From | Waiters]}};

handle_call(wait_for_ready, _From, #state{state = work} = State) ->
    {reply, ready, State};

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
handle_info(init, #state{state = init} = State) ->
    #state{
       conn_attrs = ConnAttrs,
       keepalive = KeepAlive,
       connect_timeout = ConnectTimeout,
       connect_limit = ConnectLimit,
       connect_number = ConnectNumber
      } = State,

    case amqp_connection:start(ConnAttrs) of
	{ok, Conn} ->
	    ConnMonitor = monitor(process, Conn),
	    State1 = State#state{
		       state = start,
		       conn = Conn,
		       conn_monitor = ConnMonitor,
		       connect_number = 0
		      },
	    self() ! start,
	    {noreply, State1};

	_Error when KeepAlive, ConnectLimit == infinity orelse (ConnectNumber + 1) < ConnectLimit ->
	    State1 = State#state{
		       connect_number = ConnectNumber + 1
		      },
	    timer:send_after(ConnectTimeout, init),
	    {noreply, State1};

	Error ->
	    {stop, {error, {connect_to_amqp_server, Error}}, State}
    end;

handle_info(start, #state{state = start} = State) ->
    #state{
       sup = SupPid,
       cb_module = CbModule,
       cb_args = CbArgs,
       conn = Conn,
       exchange_declare = ExchangeDeclare,
       queue_declare = QueueDeclare,
       routing_key = RoutingKey,
       consumers_number = NumberOfConsumers,
       waiters = Waiters
      } = State,

    {ok, Chan} = amqp_connection:open_channel(Conn),
    #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Chan, QueueDeclare),
    QueueDeclare1 = QueueDeclare#'queue.declare'{queue = Queue, passive = true},

    {ok, WorkerSup} = amqp_handler:start_worker_sup(SupPid, CbModule, CbArgs),
    {ok, _ConsumerSup} = amqp_handler:start_consumer_sup(SupPid, Conn, ExchangeDeclare, QueueDeclare1, RoutingKey, NumberOfConsumers, WorkerSup),

    amqp_channel:close(Chan),

    lists:foreach(fun (From) -> gen_server:reply(From, ready) end, Waiters),

    {noreply, State#state{queue_declare = QueueDeclare1, state = work, waiters = []}};

handle_info({'DOWN', _ConnMonitor, process, _Conn, _Info}, State) ->
    #state{
       sup = SupPid,
       keepalive = KeepAlive
      } = State,
    case KeepAlive of
	true ->
	    amqp_handler:stop_consumer_sup(SupPid),
	    amqp_handler:stop_worker_sup(SupPid),
	    State1 = State#state{
		       state = init,
		       conn = undefined,
		       conn_monitor = undefined
		      },
	    self() ! init,
	    {noreply, State1};

	false ->
	    {stop, {error, amqp_connection_process_shutdown}, State}
    end;

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
