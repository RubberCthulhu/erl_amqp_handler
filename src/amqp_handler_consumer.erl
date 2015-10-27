%%%-------------------------------------------------------------------
%%% @author Danil Onishchenko
%%% @copyright (C) 2014, Danil Onishchenko
%%% @doc
%%%
%%% @end
%%% Created : 21 Mar 2014 by Danil Onishchenko
%%%-------------------------------------------------------------------
-module(amqp_handler_consumer).

-behaviour(gen_server).

%% API
-export([start_link/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

-record(state, {
	  chan :: pid(),
	  %% chan_monitor :: reference(),
	  exchange,
	  routing_key,
	  queue,
	  consumer_tag,
	  state :: bind | consume, stop,
	  worker_sup :: pid()
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
start_link(Conn, ExchangeDeclare, QueueDeclare, RoutingKey, WorkerSup) ->
    gen_server:start_link(?MODULE, [Conn, ExchangeDeclare, QueueDeclare, RoutingKey, WorkerSup], []).

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
init([Conn, ExchangeDeclare, QueueDeclare, RoutingKey, WorkerSup]) ->
    process_flag(trap_exit, true),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    link(Chan),

    #'exchange.declare'{exchange = Exchange} = ExchangeDeclare,
    #'exchange.declare_ok'{} = amqp_channel:call(Chan, ExchangeDeclare),
    %% QueueDeclare = #'queue.declare'{exclusive = true, auto_delete = true},
    #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Chan, QueueDeclare),
    ok = create_bindings(Chan, Exchange, RoutingKey, Queue),
    #'basic.consume_ok'{consumer_tag = Tag} = amqp_channel:call(Chan, #'basic.consume'{queue = Queue}),

    %% ChanMonitor = monitor(process, Chan),
    State = #state{
	       chan = Chan,
	       %% chan_monitor = ChanMonitor,
	       exchange = ExchangeDeclare,
	       routing_key = RoutingKey,
	       queue = Queue,
	       consumer_tag = Tag,
	       state = bind,
	       worker_sup = WorkerSup
	      },

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
handle_info({'EXIT', From, _Reason}, #state{chan = Chan} = State) when From == Chan ->
    {stop, {error, amqp_channel_process_shutdown}, State};

handle_info({'EXIT', _From, Reason}, State) ->
    {stop, Reason, State};

%% handle_info({'DOWN', ChanMonitor, process, Chan, _Info},
%% 	    #state{chan_monitor = ChanMonitor, chan = Chan} = State) ->
%%     {stop, {error, amqp_channel_process_shutdown}, State};


handle_info(#'basic.consume_ok'{consumer_tag = Tag},
	    #state{consumer_tag = Tag, state = bind} = State) ->
    State1 = State#state{state = consume},
    {noreply, State1};

handle_info(#'basic.cancel_ok'{consumer_tag = Tag}, 
	    #state{consumer_tag = Tag, state = stop} = State) ->
    %% #state{chan = Chan,
    %% 	   chan_monitor = ChanMonitor} = State,
    %% demonitor(ChanMonitor),
    #state{chan = Chan} = State,
    unlink(Chan),
    amqp_channel:close(Chan),
    {stop, normal, State};

handle_info(#'basic.return'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{consumer_tag = Tag} = Deliver, #amqp_msg{} = Msg}, 
	    #state{consumer_tag = Tag, state = consume} = State) ->
    #state{chan = Chan,
	   worker_sup = WorkerSup} = State,
    {ok, _} = amqp_handler_worker_sup:start_child(WorkerSup, Chan, Deliver, Msg),
    {noreply, State};

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
terminate(_Reason, #state{chan = Chan} = _State) ->
    unlink(Chan),
    amqp_channel:close(Chan),
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

create_bindings(Chan, Exchange, RoutingKey, Queue) ->
    Binding = #'queue.bind'{
		 exchange = Exchange,
		 queue = Queue,
		 routing_key = RoutingKey
		},
    #'queue.bind_ok'{} = amqp_channel:call(Chan, Binding),
    ok.






