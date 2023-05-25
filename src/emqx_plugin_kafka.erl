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

-module(emqx_plugin_kafka).

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
        ]).

%% Message Pubsub Hooks
-export([ on_message_publish/2
        ]).

-define(Conf, element(2, hocon:load("/etc/emqx/emqx_plugin_kafka.conf"))).

kafka_init(_Env) ->
    ?SLOG(info, "Start to init emqx plugin kafka...... ~n"),
    ?SLOG(info, #{msg => "conf", conf => ?Conf, env => _Env}),

    KafkaServers = maps:get(<<"kafka_servers">>, ?Conf),
    ServerHosts = lists:map(fun(A) -> {maps:get(<<"host">>, A), maps:get(<<"port">>, A)} end, KafkaServers),
    ?SLOG(info, #{msg => "kafka server hosts", kafkaServers => KafkaServers, serverHosts => ServerHosts}),
    {ok, _} = application:ensure_all_started(brod),
    ok = brod:start_client(ServerHosts, client),
    CommonTopic = maps:get(<<"common_topic">>, ?Conf),
    maps:foreach(fun(A,B) -> brod:start_producer(client, B, []) end, CommonTopic),
    List = maps:get(<<"topic_mapping">>, ?Conf),
    lists:foreach(fun(A) -> brod:start_producer(client, maps:get(<<"kafka_topic">>, A), []) end, List),
    ?SLOG(warning, "Init emqx plugin kafka successfully.....~n"),
    ok.
    

%% Called when the plugin application start
load(Env) ->
    kafka_init([Env]),
    hook('client.connected',    {?MODULE, on_client_connected, [Env]}),
    hook('client.disconnected', {?MODULE, on_client_disconnected, [Env]}),
    hook('message.publish',     {?MODULE, on_message_publish, [Env]}).

%%--------------------------------------------------------------------
%% Client LifeCircle Hooks
%%--------------------------------------------------------------------

on_client_connected(ClientInfo = #{clientid := ClientId}, ConnInfo, _Env) ->
    % ?SLOG(warning, #{msg=>"clientInfo", connInfo => ConnInfo}),
    Ts = maps:get(connected_at, ConnInfo),
    Username = maps:get(username, ConnInfo),
    Action = <<"connected">>,
    Keepalive = maps:get(keepalive, ConnInfo),
    {IpAddr, _Port} = maps:get(peername, ConnInfo),
    IsSuperuser = maps:get(is_superuser, ClientInfo),
    Payload = [
        {action, Action},
        {username, Username},
        {keepalive, Keepalive},
        {ipaddress, iolist_to_binary(ntoa(IpAddr))},
        {ts, Ts},
        {client_id, ClientId}
    ],

    MqttClientConnected = get_common_topic(<<"client_connected">>),

    if
        not IsSuperuser ->
            send_kafka(Payload, Username, MqttClientConnected);
        true -> ok
    end.

on_client_disconnected(ClientInfo = #{clientid := ClientId}, ReasonCode, ConnInfo, _Env) ->
    Ts = maps:get(connected_at, ConnInfo),
    Username = maps:get(username, ConnInfo),
    Action = <<"disconnected">>,
    IsSuperuser = maps:get(is_superuser, ClientInfo),
    Payload = [
        {action, Action},
        {username, Username},
        {client_id, ClientId},
        {reason, ReasonCode},
        {ts, Ts}
    ],

    MqttClientDisconnected = get_common_topic(<<"client_disconnected">>),

    if 
        not IsSuperuser ->
            send_kafka(Payload, Username, MqttClientDisconnected);
        true -> ok
    end.

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
        {username, Username},
        {topic, Topic},
        {action, MsgType},
        {client_id, From},
        {qos, Qos},
        {retain, Retain}
    ],

    TopicMapping = maps:get(<<"topic_mapping">>, ?Conf),
    lists:foreach(fun(A) -> 
        MqttTopic = maps:get(<<"mqtt_topic">>, A),
        case emqx_topic:match(Topic, MqttTopic) of
            true -> 
                KafkaTopic = maps:get(<<"kafka_topic">>, A),
                send_kafka(MsgBody, Username, KafkaTopic);
            false -> ok
        end end,
        TopicMapping
        ).
    


send_kafka(MsgBody, Username, KafkaTopic) -> 
    Mb = jsx:encode(MsgBody),
    PayloadJson = iolist_to_binary(Mb),
    brod:produce_cb(client, KafkaTopic, hash, Username, PayloadJson, fun(_,_) -> ok end),
    % ?SLOG(warning, #{msg => "send kafka", kafkaTopic => KafkaTopic}),
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

get_common_topic(HookName) ->
    CommonTopic = maps:get(<<"common_topic">>, ?Conf),
    maps:get(HookName, CommonTopic).

%% Called when the plugin application stop
unload() ->
    unhook('client.connected',    {?MODULE, on_client_connected}),
    unhook('client.disconnected', {?MODULE, on_client_disconnected}),
    unhook('message.publish',     {?MODULE, on_message_publish}).

hook(HookPoint, MFA) ->
    %% use highest hook priority so this module's callbacks
    %% are evaluated before the default hooks in EMQX
    emqx_hooks:add(HookPoint, MFA, _Property = ?HP_HIGHEST).

unhook(HookPoint, MFA) ->
    emqx_hooks:del(HookPoint, MFA).
