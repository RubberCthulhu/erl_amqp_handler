%%%-------------------------------------------------------------------
%%% @author Danil Onishchenko
%%% @copyright (C) 2014, Danil Onishchenko
%%% @doc
%%%
%%% @end
%%% Created : 20 Mar 2014 by Danil Onishchenko
%%%-------------------------------------------------------------------
-module(amqp_handler_worker).

-behaviour(gen_server).

%% API
-export([start_link/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-include_lib("amqp_client/include/amqp_client.hrl").

%%-define(SERVER, ?MODULE).

-record(state, {
	  amqp_channel,
	  amqp_deliver,
	  amqp_msg,
	  cb_module,
	  cb_state
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
start_link(CbModule, CbArgs, Chan, Deliver, Msg) ->
    gen_server:start_link(?MODULE, [CbModule, CbArgs, Chan, Deliver, Msg], []).

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
init([CbModule, CbArgs, Chan, Deliver, Msg]) ->
    case CbModule:init(CbArgs) of
	{ok, CbState} ->
	    State = #state{
		       amqp_channel = Chan,
		       amqp_deliver = Deliver,
		       amqp_msg = Msg,
		       cb_module = CbModule,
		       cb_state = CbState
		      },
	    self() ! work,
	    {ok, State};
	Error ->
	    {stop, Error}
    end.

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
handle_info(work, State) ->
    #state{
       amqp_channel = Chan,
       amqp_deliver = Deliver,
       amqp_msg = Msg,
       cb_module = CbModule,
       cb_state = CbState
      } = State,
    #'basic.deliver'{
       exchange = Exchange,
       routing_key = RoutingKey,
       delivery_tag = DeliveryTag
      } = Deliver,
    #amqp_msg{
       props = Props
      } = Msg,
    #'P_basic'{
       reply_to = ReplyTo,
       correlation_id = CorrelationId
      } = Props,

    amqp_channel:cast(Chan, #'basic.ack'{delivery_tag = DeliveryTag}),

    Reason = case CbModule:handle(Exchange, RoutingKey, Msg, CbState) of
		 {reply, Reply, _CbState1} ->
		     case amqp_reply(Chan, Exchange, ReplyTo, CorrelationId, Reply) of
			 ok ->
			     normal;
			 Error ->
			     Error
		     end;
		 {noreply, _CbState1} ->
		     normal;
		 {stop, Reason1} ->
		     Reason1;
		 {error, _} = Error ->
		     Error
	     end,
    {stop, Reason, State};
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
terminate(Reason, #state{cb_module = CbModule, cb_state = CbState} = _State) ->
    CbModule:terminate(Reason, CbState),
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

amqp_reply(_, _, undefined, _, _) ->
    {error, undefined_reply_to};
amqp_reply(Chan, Exchange, RoutingKey, CorrelationId, Reply) ->
    Publish = #'basic.publish'{exchange = Exchange, routing_key = RoutingKey},
    Msg = case Reply of
	      {ContentType, Payload} ->
		  Props = #'P_basic'{content_type = ContentType, correlation_id = CorrelationId},
		  #amqp_msg{props = Props, payload = Payload};
	      Payload ->
		  Props = #'P_basic'{correlation_id = CorrelationId},
		  #amqp_msg{props = Props, payload = Payload}
	  end,

    amqp_channel:cast(Chan, Publish, Msg),
    ok.






