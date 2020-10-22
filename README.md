# Zaark AMQP client

## Config

In your sys.config or .app.src file add this configuration:

Zamqp can connect to many rmq host that are specified in the servers list.
Publishsers in the config is a list of maps that contain the key exchange that point to what exchanges the node will publish to.
Consumers in the config is a list of maps that contain what module is the consumer and what the routing_key is and what exchange the consumer will consume from.

```erlang
 {zamqp, [{servers, [
                     #{user => <<"username">>,
                       pass => <<"password">>,
                       host => "host1",
                       port => 5672
                      },
                     #{user => <<"username">>,
                       pass => <<"password">>,
                       host => "host2",
                       port => 5672
                      }                    ]},
          {publishers, [#{exchange => myexchange}] },
          {consumers, [#{module => my_consumer_module,
                         routing_key => <<"#">>,
                         exchange => <<"myexchange">>}] }
         ]
 },

```

## Create a consumer
Example consumer:

```erlang
-module(my_consumer).
-behaviour(zamqp_consumer).

%% zamqp_consumer callbacks
-export([handle/1]).

%% ===================================================================
%% zamqp_consumer callbacks
%% ===================================================================
handle(Event) ->
    io:format("Call: ~p~n", [Event]).
    

```

## Publish a message
In the code where the publish should be done this can be used:

Event is the json that we want to send on a exchange, with a routing key.

The UUID is more of a correlation id if it want to be tracked in the other end.

```erlang
zamqp:publish(Exchange, RoutingKey, Event, UUID)
'```

## Body

Zamqp doesn't decode or require any format in the Event section so what is sent on the exchange is what consumer will get.