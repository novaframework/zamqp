-module(zamqp_conn_tests).

%% Includes
-include_lib("eunit/include/eunit.hrl").

%% Defines

%% ===================================================================
%% Tests.
%% ===================================================================

simple_test_() ->
    Args = #{user => <<"guest">>,
             pass => <<"guest">>,
             host => {127, 0, 0, 1},
             port => 5672},
    [
     {"Open",
      ?_test(?assertEqual(ok,
                          begin
                              {ok, _, Con} = zamqp_conn:open(Args),
                              zamqp_conn:close(Con)
                          end))},
     {"Exchange",
      ?_test(
         ?assertEqual(
            ok,
            begin
                {ok, _, Con} = zamqp_conn:open(Args),
                {ok, Con1} = zamqp_conn:declare(exchange, foo, Con),
                {ok, Con2} = zamqp_conn:delete(exchange, foo, Con1),
                zamqp_conn:close(Con2)
            end))},
     {"Queue",
      ?_test(
         ?assertEqual(
            ok,
            begin
                {ok, _, Con} = zamqp_conn:open(Args),
                {ok, Con1} = zamqp_conn:declare(exchange, foo, Con),
                {ok, Con2} = zamqp_conn:declare(queue, bar, Con1),
                {ok, Con3} = zamqp_conn:delete(queue, bar, Con2),
                {ok, Con4} = zamqp_conn:delete(exchange, foo, Con3),
                zamqp_conn:close(Con4)
            end))},
     {"bind",
      ?_test(
         ?assertEqual(
            ok,
            begin
                {ok, _, Con} = zamqp_conn:open(Args),
                {ok, Con1} = zamqp_conn:declare(exchange, foo, Con),
                {ok, Con2} = zamqp_conn:declare(queue, bar, Con1),
                {ok, Con3} = zamqp_conn:bind(bar, foo, <<"#">>, Con2),
                {ok, Con4} = zamqp_conn:delete(queue, bar, Con3),
                {ok, Con5} = zamqp_conn:delete(exchange, foo, Con4),
                zamqp_conn:close(Con5)
            end))},
     {"consumer",
      ?_test(
         ?assertEqual(
            ok,
            begin
                {ok, _, Con} = zamqp_conn:open(Args),
                {ok, Con1} = zamqp_conn:declare(exchange, foo, Con),
                {ok, Con2} = zamqp_conn:declare(queue, bar, Con1),
                {ok, Con3} = zamqp_conn:bind(bar, foo, <<"#">>, Con2),
                {ok, Tag, Con4} = zamqp_conn:consume(bar, Con3),
                {ok, Con5} = zamqp_conn:cancel(consumer, Tag, Con4),
                {ok, Con6} = zamqp_conn:delete(queue, bar, Con5),
                {ok, Con7} = zamqp_conn:delete(exchange, foo, Con6),
                zamqp_conn:close(Con7)
            end))}
    ].

publish_test_() ->
    Args = #{user => <<"guest">>,
             pass => <<"guest">>,
             host => {127, 0, 0, 1},
             port => 5672},
    [{"send",
      ?_test(?assertEqual(ok,
                          begin
                              {ok, _, Con} = zamqp_conn:open(Args),
                              {ok, Con1} =
                                  zamqp_conn:declare(exchange, send_test, Con),
                              zamqp_conn:publish(send_test,
                                                 <<"a.key">>,
                                                 <<"Test">>,
                                                 Con1),
                              {ok, Con2} =
                                  zamqp_conn:delete(exchange, send_test, Con1),
                              zamqp_conn:close(Con2)
                          end))}
    ].

consume_test_() ->
    Args = #{user => <<"guest">>,
             pass => <<"guest">>,
             host => {127, 0, 0, 1},
             port => 5672},
    Large = crypto:rand_bytes(1024 * 2014),
    [{"regular",
      ?_test(
         ?assertEqual(
            ok,
            begin
                {ok, _, Con} = zamqp_conn:open(Args),
                {ok, Con1} = zamqp_conn:declare(exchange, foo, Con),
                {ok, Con2} = zamqp_conn:declare(queue, bar, Con1),
                {ok, Con3} = zamqp_conn:bind(bar, foo, <<"test.#">>, Con2),
                {ok, CTag, Con4} = zamqp_conn:consume(bar, Con3),
                zamqp_conn:publish(foo, <<"test.key">>, <<"Test">>, Con4),
                zamqp_conn:active_once(Con3),
                Bin = receive {tcp, _, Bin0}  -> Bin0
                      after 1000 -> erlang:error(timeout)
                      end,
                {ok,
                 #{delivery_tag := DTag,
                   routing_key := <<"test.key">>,
                   body := <<"Test">>},
                 Con5} = zamqp_conn:ingest(Bin, Con4),
                zamqp_conn:ack(DTag, Con4),
                {ok, Con6} = zamqp_conn:cancel(consumer, CTag, Con5),
                {ok, Con7} = zamqp_conn:delete(queue, bar, Con6),
                {ok, Con8} = zamqp_conn:delete(exchange, foo, Con7),
                zamqp_conn:close(Con8)
            end))},
     {"large",
      ?_test(
         ?assertEqual(
            ok,
            begin
                {ok, _, Con} = zamqp_conn:open(Args),
                {ok, Con1} = zamqp_conn:declare(exchange, foo, Con),
                {ok, Con2} = zamqp_conn:declare(queue, bar, Con1),
                {ok, Con3} = zamqp_conn:bind(bar, foo, <<"test.#">>, Con2),
                {ok, CTag, Con4} = zamqp_conn:consume(bar, Con3),
                zamqp_conn:publish(foo, <<"test.key">>, Large, Con4),
                zamqp_conn:active_once(Con3),
                Bin = receive {tcp, _, Bin0}  -> Bin0
                      after 1000 -> erlang:error(timeout)
                      end,
                {ok,
                 #{delivery_tag := DTag,
                   routing_key := <<"test.key">>,
                   body := Large},
                 Con5} = zamqp_conn:ingest(Bin, Con4),
                zamqp_conn:ack(DTag, Con4),
                {ok, Con6} = zamqp_conn:cancel(consumer, CTag, Con5),
                {ok, Con7} = zamqp_conn:delete(queue, bar, Con6),
                {ok, Con8} = zamqp_conn:delete(exchange, foo, Con7),
                zamqp_conn:close(Con8)
            end))}
    ].

heartbeat_test_() ->
    Args = #{user => <<"guest">>,
             pass => <<"guest">>,
             host => {127, 0, 0, 1},
             port => 5672},
    [{"send",
      ?_test(?assertEqual(ok,
                          begin
                              {ok, _, Con} = zamqp_conn:open(Args),
                              zamqp_conn:heartbeat(Con),
                              zamqp_conn:close(Con)
                          end))}
    ].

%% ===================================================================
%% Internal functions.
%% ===================================================================
