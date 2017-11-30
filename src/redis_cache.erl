%%%-------------------------------------------------------------------
%%% @author ccredrock@gmail.com
%%% @copyright (C) 2017, <free>
%%% @doc
%%%
%%% @end
%%% Created : 2017年07月05日19:11:34
%%%-------------------------------------------------------------------
-module(redis_cache).

-export([start/0, stop/0]).

-export([start_link/1]).

-export([put_val/1,     %% 更新
         put_val/3,     %% 更新
         put_val/4,     %% 更新
         del_val/1,     %% 删除
         del_val/2,     %% 删除
         set_val/1,     %% 更新 || 删除
         get_val/1,     %% 获取
         get_val/2,     %% 获取
         get_val/3]).   %% 获取

-export([get_ref/0,      %% 索引
         diff_ref/2,     %% 索引
         rset_val/2,     %% 更新 || 删除
         rput_val/2,     %% 更新
         rput_val/4,     %% 更新
         rput_val/5,     %% 更新
         rdel_val/2,     %% 删除
         rdel_val/3]).   %% 删除

-export([purge/0,       %% 净化系统
         clean/0,       %% 清理数据
         clean/1,       %% 清理数据
         reload/0,      %% 重新加载
         reload/1]).    %% 重新加载

%% config
-export([loop_time/0,      %% config: table process loop time
         alarm_len/0,      %% config: version list alarm len
         clean_len/0]).    %% config: clean clean len

-export([get_redis_notice_len/0,
         get_cache_notice_len/0,
         get_table_vals/1,
         get_redis_vals/1]).

%% callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%------------------------------------------------------------------------------
-behaviour(gen_server).

-define(LOOP_TIME,  50).
-define(ALARM_LEN,  100000).
-define(CLEAN_LEN,  70000). % error when over 35000/50ms

-define(ETS, '$redis_cache').
-define(ETS_TABLE(L), list_to_atom("$redis_cache_kv#" ++ atom_to_list(L))).
-define(REDIS_TABLE(L), iolist_to_binary([<<"${redis_cache}_kv#">> | L])).

-define(NOTICE,  <<"${redis_cache}_notice">>).

-define(BIN(V), to_binary(V)).

-record(state, {tables     = [],    %% 所有关心的表
                notice_len = 0}).   %% 清理单位长度

%%------------------------------------------------------------------------------
start() ->
    application:ensure_all_started(?MODULE).

stop() ->
    application:stop(?MODULE).

%%------------------------------------------------------------------------------
%% config
%%------------------------------------------------------------------------------
-spec loop_time() -> pos_integer().
loop_time() ->
    application:get_env(redis_cache, loop_time, ?LOOP_TIME).

-spec alarm_len() -> pos_integer().
alarm_len() ->
    application:get_env(redis_cache, alarm_len, ?ALARM_LEN).

-spec clean_len() -> pos_integer().
clean_len() ->
    application:get_env(redis_cache, clean_len, ?CLEAN_LEN).

%%------------------------------------------------------------------------------
-spec start_link([atom()]) -> {ok, pid()} | ignore | {error, any()}.
start_link(Tables) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Tables], []).

%%------------------------------------------------------------------------------
-spec put_val(atom(), binary(), any(), any()) -> ok | {'EXIT', any()}.
put_val(Table, Key, CKey, CVal) ->
    put_val(Table, Key, [{CKey, CVal}]).

%% [{ckey, cval}]
put_val(Table, Key, MKV) ->
    put_val([{Table, Key, MKV}]).

%% [{table, key, [{ckey, cval}]}]
put_val(List) ->
    set_val([{put, T, K, M} || {T, K, M} <- List]).

del_val(Table, Key) ->
    del_val([{Table, Key}]).

%% [{table, key}]
del_val(List) ->
    set_val([{del, T, K} || {T, K} <- List]).

%% [{op, ...}]
set_val(List) ->
    gen_server:call(?MODULE, {set_val, List}).

get_val(Table) ->
    ets:tab2list(?ETS_TABLE(Table)).

get_val(Table, Key) ->
    case ets:lookup(?ETS_TABLE(Table), Key) of
        [{Key, Map}] -> Map;
        _ -> null
    end.

get_val(Table, Key, CKey) ->
    case get_val(Table, Key) of
        null -> null;
        Map -> maps:get(CKey, Map, null)
    end.

%%------------------------------------------------------------------------------
get_ref() ->
    [{ref, Ref}] = ets:lookup(?ETS, ref), Ref.

%% [{op, ...}]
rset_val(Ref, List) ->
    gen_server:call(?MODULE, {rset_val, Ref, List}).

diff_ref(From, From) -> [];
diff_ref(From, To) when From >= 0 andalso To >= 0 ->
    gen_server:call(?MODULE, {diff_ref, From, To}).

-spec rput_val(atom(), binary(), any(), any()) -> ok | {'EXIT', any()}.
rput_val(Ref, Table, Key, CKey, CVal) ->
    rput_val(Ref, Table, Key, [{CKey, CVal}]).

%% [{ckey, cval}]
rput_val(Ref, Table, Key, MKV) ->
    rput_val(Ref, [{Table, Key, MKV}]).

%% [{table, key, [{ckey, cval}]}]
rput_val(Ref, List) ->
    rset_val(Ref, [{put, T, K, M} || {T, K, M} <- List]).

rdel_val(Ref, Table, Key) ->
    rdel_val(Ref, [{Table, Key}]).

%% [{table, key}]
rdel_val(Ref, List) ->
    rset_val(Ref, [{del, T, K} || {T, K} <- List]).

%%------------------------------------------------------------------------------
reload() -> reload('$all').
reload(Table) when is_atom(Table) -> reload([Table]);
reload(List) -> gen_server:call(?MODULE, {reload, List}).

clean() -> clean('$all').
clean(Table) when is_atom(Table) -> clean([Table]);
clean(List) -> gen_server:call(?MODULE, {clean, List}).

purge() ->
    {ok, Tables} = application:get_env(redis_cache, tables),
    {ok, _} = eredis_cluster:q([<<"DEL">>, ?NOTICE]),
    do_clean_table(Tables), ok.

get_redis_notice_len() ->
    {ok, BLen} = eredis_cluster:q([<<"LLEN">>, ?NOTICE]), binary_to_integer(BLen).

get_cache_notice_len() ->
    State = sys:get_state(?MODULE), State#state.notice_len.

get_table_vals(Table) ->
    ets:tab2list(?ETS_TABLE(Table)).

get_redis_vals(Table) ->
    {ok, KeyList} = eredis_cluster:q([<<"KEYS">>, ?REDIS_TABLE([?BIN(Table), "@", <<"*">>])]),
    [begin
         {ok, Vals} = eredis_cluster:q([<<"HGETALL">>, ?REDIS_TABLE([?BIN(Table), "@", ?BIN(Key)])]),
         {Key, Vals}
     end || Key <- KeyList].

%%------------------------------------------------------------------------------
init([Tables]) ->
    Len = do_load_len(),
    [do_load_table(Table) || Table <- Tables],
    {ok, #state{tables = Tables, notice_len = Len}, 0}.

handle_call({set_val, List}, _, State) ->
    {reply, catch do_set_val(List), State};
handle_call({rset_val, Ref, List}, _, State) ->
    case catch do_rset_val(State, Ref, List) of
        {'EXIT', _} = Result -> {reply, Result, State};
        State1 -> {reply, ok, State1}
    end;
handle_call({reload, ['$all']}, _From, State) ->
    {reply, catch do_reload_table(State#state.tables), State};
handle_call({reload, List}, _From, State) ->
    {reply, catch do_reload_table(List), State};
handle_call({clean, ['$all']}, _From, State) ->
    {reply, catch do_clean_table(State#state.tables), State};
handle_call({clean, List}, _From, State) ->
    {reply, catch do_clean_table(List), State};
handle_call({diff_ref, Old, New}, _, State) ->
    {reply, catch do_diff_ref(Old, New, State), State};
handle_call(_Call, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_info(timeout, State) ->
    State1 = do_timeout(State),
    erlang:send_after(loop_time(), self(), timeout),
    {noreply, State1};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
to_binary(X) when is_atom(X)    -> list_to_binary(atom_to_list(X));
to_binary(X) when is_integer(X) -> integer_to_binary(X);
to_binary(X) when is_binary(X)  -> X;
to_binary(X) ->
    case io_lib:printable_list(X) of
        true -> list_to_binary(X);
        false -> jsx:encode(X)
    end.

get_type(X) when is_binary(X) -> <<"binary">>;
get_type(X) when is_integer(X) -> <<"integer">>;
get_type(X) when is_atom(X) -> <<"atom">>;
get_type(X) ->
    case io_lib:printable_list(X) of
        true -> <<"string">>;
        false -> <<"json">>
    end.

put_type(X, binary) -> X;
put_type(X, <<"binary">>) -> X;
put_type(X, string) -> binary_to_list(X);
put_type(X, <<"string">>) -> binary_to_list(X);
put_type(X, integer) -> binary_to_integer(X);
put_type(X, <<"integer">>) -> binary_to_integer(X);
put_type(X, atom) -> list_to_atom(binary_to_list(X));
put_type(X, <<"atom">>) -> list_to_atom(binary_to_list(X));
put_type(X, _T) when not is_binary(X) -> X;
put_type(X, T) when T =:= json orelse T =:= <<"json">> -> jsx:decode(X, [return_maps]).

encode(V) ->
    jsx:encode(#{<<"type">> => get_type(V), <<"val">> => to_binary(V)}).

decode(B) ->
    #{<<"type">> := T, <<"val">> := V} = jsx:decode(B, [return_maps]),
    put_type(V, T).

%%------------------------------------------------------------------------------
do_load_table(Table) ->
    EtsTable = ?ETS_TABLE(Table),
    ets:new(EtsTable, [named_table, public, {read_concurrency, true}]),
    {ok, List} = eredis_cluster:q([<<"KEYS">>, ?REDIS_TABLE([?BIN(Table), "@", <<"*">>])]),
    [do_load_table_key(EtsTable, RedisTable) || RedisTable <- List].

do_load_table_key(EtsTable, RedisTable) ->
    {ok, List} = eredis_cluster:q([<<"HGETALL">>, RedisTable]),
    {_Table, EtsKey} = do_parse_redis_table(RedisTable),
    ets:insert(EtsTable, {EtsKey, do_form_map(List, maps:new())}).

do_form_map([Key, Val | T], Map) ->
    do_form_map(T, Map#{decode(Key) => decode(Val)});
do_form_map([], Map) -> Map.

do_reload_table([Table | T]) ->
    EtsTable = ?ETS_TABLE(Table),
    ets:delete_all_objects(EtsTable),
    {ok, List} = eredis_cluster:q([<<"KEYS">>, ?REDIS_TABLE([?BIN(Table), "@", <<"*">>])]),
    [do_load_table_key(EtsTable, RedisTable) || RedisTable <- List],
    do_reload_table(T);
do_reload_table([]) -> ok.

do_clean_table([Table | T]) ->
    EtsTable = ?ETS_TABLE(Table),
    ets:info(EtsTable) =/= undefined andalso ets:delete_all_objects(EtsTable),
    {ok, List} = eredis_cluster:q([<<"KEYS">>, ?REDIS_TABLE([?BIN(Table), "@", <<"*">>])]),
    [{ok, _} = eredis_cluster:transaction([[<<"RPUSH">>, ?NOTICE, jsx:encode(#{<<"op">> => <<"del">>, <<"table">> => X})],
                                           [<<"DEL">>, X]]) || X <- List],
    do_clean_table(T);
do_clean_table([]) -> ok.

do_load_len() ->
    ets:new(?ETS, [named_table, public, {read_concurrency, true}]),
    {ok, BinLen} = eredis_cluster:q([<<"LLEN">>, ?NOTICE]),
    Len = binary_to_integer(BinLen),
    ets:insert_new(?ETS, {ref, Len}), Len.

%%------------------------------------------------------------------------------
do_set_val([]) -> skip;
do_set_val(List) ->
    do_set_redis(List, []),
    do_set_ets(List), ok.

do_set_redis([{put, Table, Key, MKV} | T], Acc) ->
    RedisTable = ?REDIS_TABLE([?BIN(Table), "@", get_type(Key), "&", ?BIN(Key)]),
    do_set_redis(T, Acc ++
                 [[<<"RPUSH">>, ?NOTICE, jsx:encode(#{<<"op">> => <<"put">>, <<"table">> => RedisTable})],
                  [<<"HMSET">>, RedisTable] ++ lists:flatten([[encode(CK), encode(CV)] || {CK, CV} <- MKV])]);
do_set_redis([{del, Table, Key} | T], Acc) ->
    RedisTable = ?REDIS_TABLE([?BIN(Table), "@", get_type(Key), "&", ?BIN(Key)]),
    do_set_redis(T, Acc ++
                 [[<<"RPUSH">>, ?NOTICE, jsx:encode(#{<<"op">> => <<"del">>, <<"table">> => RedisTable})],
                  [<<"DEL">>, RedisTable]]);
do_set_redis([], Acc) -> {ok, _} = eredis_cluster:transaction(Acc).

do_set_ets([{put, Table, Key, MKV} | T]) ->
    EtsTable = ?ETS_TABLE(Table),
    ets:insert(EtsTable, {Key, do_get_map(EtsTable, Key, MKV)}),
    do_set_ets(T);
do_set_ets([{del, Table, Key} | T]) ->
    ets:delete(?ETS_TABLE(Table), Key),
    do_set_ets(T);
do_set_ets([]) -> skip.

do_get_map(Name, Key, MKV) ->
    case ets:lookup(Name, Key) of
        [] -> maps:from_list(MKV);
        [{_Key, Map}] -> maps:merge(Map, maps:from_list(MKV))
    end.

%%------------------------------------------------------------------------------
do_rset_val(State, _Ref, []) -> State;
do_rset_val(#state{notice_len = Len} = State, Ref, List) ->
    RedisLen = do_rset_redis(Ref, List, [], 1, [], 1, []),
    do_set_ets(List),
    case binary_to_integer(RedisLen) =:= Len of
        true ->
            NLen = Len + length(List),
            ets:insert(?ETS, {ref, NLen}),
            State#state{notice_len = NLen};
        false ->
            State
    end.

do_rset_redis(Ref, [{put, Table, Key, MKV} | T], Str, KeyNth, Keys, ArgNth, Args) ->
    RedisTable = ?REDIS_TABLE([?BIN(Table), "@", get_type(Key), "&", ?BIN(Key)]),
    {ArgNth1, ArgsStr} = do_form_args(MKV, ArgNth, []),
    do_rset_redis(Ref, T,
                  Str ++ ["redis.pcall('HMSET', KEYS[", ?BIN(KeyNth + 1), "]", ArgsStr, ")
                           redis.pcall('RPUSH', KEYS[1], ARGV[", ?BIN(ArgNth1 + 1), "])"],
                  KeyNth + 1,
                  Keys ++ [RedisTable],
                  ArgNth1 + 1,
                  Args
                  ++ lists:flatten([[encode(CK), encode(CV)] || {CK, CV} <- MKV])
                  ++ [jsx:encode(#{<<"op">> => <<"put">>, <<"table">> => RedisTable})]);
do_rset_redis(Ref, [{del, Table, Key} | T], Str, KeyNth, Keys, ArgNth, Args) ->
    RedisTable = ?REDIS_TABLE([?BIN(Table), "@", get_type(Key), "&", ?BIN(Key)]),
    do_rset_redis(Ref, T,
                  Str ++ ["redis.pcall('DEL', KEYS[", ?BIN(KeyNth + 1), "])
                           redis.pcall('RPUSH', KEYS[1], ARGV[", ?BIN(ArgNth + 1), "])"],
                  KeyNth + 1,
                  Keys ++ [RedisTable],
                  ArgNth + 1,
                  Args
                  ++ [jsx:encode(#{<<"op">> => <<"del">>, <<"table">> => RedisTable})]);
do_rset_redis(Ref, [], Str, KeyNth, Keys, _ArgNth, Args) ->
    Exec = [<<"eval">>,
            iolist_to_binary(["local Len = redis.pcall('LLEN', KEYS[1])
                               if Len == tonumber(ARGV[1]) then
                                ", Str, "
                                return {'OK', Len}
                              else
                               return {'ERROR', Len}
                              end"])]
            ++ [KeyNth, ?NOTICE | Keys] ++ [?BIN(Ref) | Args],
    {ok, [<<"OK">>, Len]} = eredis_cluster:q(Exec), Len.

do_form_args([_ | T], Nth, Acc) ->
    do_form_args(T, Nth + 2, [[", ARGV[", ?BIN(Nth + 1), "], ARGV[", ?BIN(Nth + 2), "]"] | Acc]);
do_form_args([], Nth, Acc) -> {Nth, lists:reverse(Acc)}.

%%------------------------------------------------------------------------------
do_timeout(State) ->
    case catch do_check_update(State) of
        #state{} = NState -> NState;
        {'EXIT', Reason} -> error_logger:error_msg("redis_cache error ~p~n", [{Reason}]), State
    end.

%% reduce_max
do_check_update(#state{notice_len = Len, tables = Tables} = State) ->
    AlarmLen = alarm_len(),
    {ok, NBinLen} = eredis_cluster:q([<<"LLEN">>, ?NOTICE]),
    case binary_to_integer(NBinLen) of
        Len ->
            State;
        NLen when NLen >= AlarmLen ->
            do_reduce_len(AlarmLen), State;
        NLen when NLen > Len ->
            do_update_len(Len, NLen, Tables),
            ets:insert(?ETS, {ref, NLen}),
            State#state{notice_len = NLen};
        NLen ->
            do_update_len(Len - AlarmLen, NLen, Tables),
            ets:insert(?ETS, {ref, NLen}),
            State#state{notice_len = NLen}
    end.

do_reduce_len(AlarmLen) ->
    CleanLen = clean_len(),
    EvalStr = ["local Len = redis.pcall('LLEN', KEYS[1])
                if Len >= tonumber(ARGV[1]) then
                    local LeftLen = Len - tonumber(ARGV[2])
                    redis.pcall('LTRIM', KEYS[1], 0 - LeftLen,  -1)
                    return {Len, LeftLen}
                else
                    return Len
                end"],
    {ok, _} = eredis_cluster:q([<<"eval">>, iolist_to_binary(EvalStr), 1, ?NOTICE, AlarmLen, CleanLen]).

do_update_len(Len, NLen, Tables) ->
    {ok, List} = eredis_cluster:q([<<"LRANGE">>, ?NOTICE, Len, NLen - 1]),
    [do_update_key(Val, Tables) || Val  <- lists:usort(List)].

do_update_key(Val, Tables) ->
    #{<<"op">> := OP, <<"table">> := RedisTable} = jsx:decode(Val, [return_maps]),
    {Table, EtsKey} = do_parse_redis_table(RedisTable),
    case lists:member(Table, Tables) of
        true ->
            EtsTable = ?ETS_TABLE(Table),
            case OP of
                <<"put">> ->
                    {ok, List} = eredis_cluster:q([<<"HGETALL">>, RedisTable]),
                    ets:insert(EtsTable, {EtsKey, do_form_map(List, maps:new())});
                <<"del">> ->
                    ets:delete(EtsTable, EtsKey)
            end;
        false ->
            skip
    end.

do_parse_redis_table(RedisTable) ->
    [<<>>, Left] = binary:split(RedisTable, ?REDIS_TABLE(<<>>)),
    [Table, Left1] = re:split(Left, <<"@">>),
    [Type, BinEtsKey] = re:split(Left1, <<"&">>),
    {put_type(Table, atom), put_type(BinEtsKey, Type)}.

%%------------------------------------------------------------------------------
do_diff_ref(Old, New, #state{tables = Tables}) ->
    {From, To} = do_from_to(Old, New),
    {ok, List} = eredis_cluster:q([<<"LRANGE">>, ?NOTICE, From, To - 1]),
    do_form_list(Tables, List, []).

do_from_to(Old, New) ->
    CleanLen = clean_len(),
    case Old =< New of
        true -> {Old, New};
        false -> {Old - CleanLen, New}
    end.

do_form_list(Tables, [Val | T], Acc) ->
    #{<<"op">> := OP, <<"table">> := RedisTable} = jsx:decode(Val, [return_maps]),
    {Table, EtsKey} = do_parse_redis_table(RedisTable),
    case lists:member(Table, Tables) of
        true -> do_form_list(Tables, T, [{OP, Table, EtsKey} | Acc]);
        false -> do_form_list(Tables, T, Acc)
    end;
do_form_list(_Tables, [], Acc) -> Acc.

