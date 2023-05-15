%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugin_template).

%% for #message{} record
%% no need for this include if we call emqx_message:to_map/1 to convert it to a map
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

%% for logging
-include_lib("emqx/include/logger.hrl").

-export([ load/1
        , unload/0
        ]).

%% Client Lifecircle Hooks
-export([ on_client_connected/3
        , on_client_disconnected/4
        , on_client_subscribe/4
        , on_client_unsubscribe/4
        ]).

%% Session Lifecircle Hooks
-export([ on_session_created/3
        , on_session_subscribed/4
        , on_session_unsubscribed/4
        ]).

%% Message Pubsub Hooks
-export([ on_message_publish/2
        ]).

func([]) -> ok;

func([H | T]) ->
    MqttTopic = maps:get(<<"mqtt_topic">>, H),
    KafkaTopic = maps:get(<<"kafka_topic">>, H),
    ?SLOG(warning, #{msg => "array topic print", mqttTopic => MqttTopic, kafkaTopic => KafkaTopic}),
    brod:start_producer(client, KafkaTopic, []),
    func(T).

kafka_init(_Env) ->
    ?SLOG(warning, "Start to init emqx plugin kafka...... ~n"),
    {ok, Conf} = hocon:load("/etc/emqx/kafka.conf"),
    KafkaServer = maps:get(<<"kafka_server">>, Conf),
    ?SLOG(warning, #{msg => "conf", conf => Conf}),

    KafkaServers = maps:get(<<"kafka_servers">>, Conf),
    List = maps:get(<<"topic_mapping">>, Conf),
    func(List),
    % Port = maps:get(<<"port">>, Conf),
    {ok, _} = application:ensure_all_started(brod),
    ok = brod:start_client(KafkaServers, client),
    ?SLOG(info, "Init emqx plugin kafka successfully.....~n"),
    ok.
    

%% Called when the plugin application start
load(Env) ->
    kafka_init([Env]),
    hook('client.connected',    {?MODULE, on_client_connected, [Env]}),
    hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),
    hook('client.subscribe',    {?MODULE, on_client_subscribe, [Env]}),
    hook('client.unsubscribe',  {?MODULE, on_client_unsubscribe, [Env]}),
    hook('session.created',     {?MODULE, on_session_created, [Env]}),
    hook('session.subscribed',  {?MODULE, on_session_subscribed, [Env]}),
    hook('session.unsubscribed',{?MODULE, on_session_unsubscribed, [Env]}),
    hook('message.publish',     {?MODULE, on_message_publish, [Env]}).

%%--------------------------------------------------------------------
%% Client LifeCircle Hooks
%%--------------------------------------------------------------------

on_client_connected(ClientInfo = #{clientid := ClientId}, ConnInfo, _Env) ->
    MqttClientConnectedTopic = maps:get(<<"client_connected">>, Conf),
    Ts = maps:get(<<"connected_at">>, ConnInfo),
    Username = maps:get(<<"username">>, ConnInfo),
    Action = <<"connected">>,
    Keepalive = maps:get(<<"keepalive">>, ConnInfo),
    {IpAddr, _Port} = maps:get(<<"peername">>, ConnInfo),
    IsSuperuser = maps:get(<<"is_superuser">>, ConnInfo),
    Payload = [
        {action, Action},
        {username, Username},
        {keeplive, Keepalive},
        {ipaddress, iolist_to_binary(ntoa(IpAddr))},
        {timestamp, Ts},
        {client_id, ClientId}
    ],
    if
        not IsSuperuser ->
            send_kafka(Payload, Username, MqttClientConnectedTopic);
        true -> ok
    end.

on_client_disconnected(ClientInfo = #{clientid := ClientId}, ReasonCode, ConnInfo, _Env) ->
    MqttClientDisconnected = maps:get(<<"client_disconnected">>),
    Ts = maps:get(<<"connected_at">>, ConnInfo),
    Username = maps:get(<<"username">>, ConnInfo),
    Action = <<"disconnected">>,
    IsSuperuser = maps:get(is_superuser, ClientInfo),
    Payload = [
        {action, Action},
        {device_id, Username},
        {client_id, ClientId},
        {reason, ReasonCode},
        {ts, Ts}
    ],
    if 
        not IsSuperuser ->
            send_kafka(Payload, Username, MqttClientDisconnected);
        true -> ok
    end.

on_client_subscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
    io:format("Client(~s) will subscribe: ~p~n", [ClientId, TopicFilters]),
    {ok, TopicFilters}.

on_client_unsubscribe(#{clientid := ClientId}, _Properties, TopicFilters, _Env) ->
    io:format("Client(~s) will unsubscribe ~p~n", [ClientId, TopicFilters]),
    {ok, TopicFilters}.

%%--------------------------------------------------------------------
%% Session LifeCircle Hooks
%%--------------------------------------------------------------------

on_session_created(#{clientid := ClientId}, SessInfo, _Env) ->
    io:format("Session(~s) created, Session Info:~n~p~n", [ClientId, SessInfo]).

on_session_subscribed(#{clientid := ClientId}, Topic, SubOpts, _Env) ->
    io:format("Session(~s) subscribed ~s with subopts: ~p~n", [ClientId, Topic, SubOpts]).

on_session_unsubscribed(#{clientid := ClientId}, Topic, Opts, _Env) ->
    io:format("Session(~s) unsubscribed ~s with opts: ~p~n", [ClientId, Topic, Opts]).

%%--------------------------------------------------------------------
%% Message PubSub Hooks
%%--------------------------------------------------------------------

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
    Timestamp = Message#message.timestamp,
    Payload = Message#message.payload,
    Username = emqx_message:get_header(username, Message),
    Topic = Message#message.topic,
    MsgType = <<"publish">>,
    From = Message#message.from,
    Qos = Message#message.qos,
    Retain = emqx_message:get_flag(retain, Message),
    MsgId = Message#message.id,

    Payload1 = json_minify(Payload),

    MsgBody = [
        {msg_id, binary:encode_hex(MsgId)},
        {ts, Timestamp},
        {payload, Payload1},
        {device_id, Username},
        {topic, Topic},
        {action, MsgType},
        {client_id, From},
        {qos, Qos},
        {retain, Retain}
    ],

    TopicStr = binary_to_list(Topic),
    


send_kafka(MsgBody, Username, KafkaTopic) -> 
    {ok, Mb} = emqx_json:safe_encode(MsgBody),
    PayloadJson = iolist_to_binary(Mb),
    brod:produce_cb(client, KafkaTopic, hash, Username, PayloadJson, fun(_,_) -> ok end),
    ok.

ntoa({0, 0, 0, 0, 0, 16#ffff, AB, CD}) ->
  inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256});
ntoa(IP) ->
  inet_parse:ntoa(IP).

json_minify(Payload)->
    IsJson = jsx:is_json(Payload),
    if 
         IsJson ->
            jsx:minify(Payload);
        true ->
            Payload
    end.

%% Called when the plugin application stop
unload() ->
    unhook('client.connected',    {?MODULE, on_client_connected}),
    unhook('client.disconnected', {?MODULE, on_client_disconnected}),
    unhook('client.subscribe',    {?MODULE, on_client_subscribe}),
    unhook('client.unsubscribe',  {?MODULE, on_client_unsubscribe}),
    unhook('session.created',     {?MODULE, on_session_created}),
    unhook('session.subscribed',  {?MODULE, on_session_subscribed}),
    unhook('session.unsubscribed',{?MODULE, on_session_unsubscribed}),
    unhook('message.publish',     {?MODULE, on_message_publish}).

hook(HookPoint, MFA) ->
    %% use highest hook priority so this module's callbacks
    %% are evaluated before the default hooks in EMQX
    emqx_hooks:add(HookPoint, MFA, _Property = ?HP_HIGHEST).

unhook(HookPoint, MFA) ->
    emqx_hooks:del(HookPoint, MFA).
