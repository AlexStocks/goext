# goext #
---
*my golang sdk package*

## dev list ##
---

- 2017/02/07
    > add broadcaster sync/broadcast.go
	>
	> do not use this again and modify all

- 2017/01/22
    > add asynchronous kafka producer in log/kafka/producer.go

- 2017/01/20
    > modify log/kafka/consumer.go

- 2017/01/14
    > add log/kafka to encapsulate kafka producer/consumer functions
    >
    > add math/rand/red_packet.go to provide tencent red packet algorithm

- 2017/01/12
    > move github.com/AlexStocks/dubbogo/common/net.go to github.com/AlexStocks/goext/net/ip.go
    >
    > move github.com/AlexStocks/dubbogo/common/misc.go(Contains, ArrayRemoveAt, RandStringBytesMaskImprSrc) to github.com/AlexStocks/goext/strings/strings.go
    >
    > move github.com/AlexStocks/dubbogo/common/misc.go(Future) to github.com/AlexStocks/goext/time/time.go
    >
    > move github.com/AlexStocks/dubbogo/common/misc.go(Goid) to github.com/AlexStocks/goext/runtime/mprof.go(GoID)

- 2016/10/23
    > move github.com/AlexStocks/pool to github.com/AlexStocks/goext/sync/pool

- 2016/10/14
    > add goext/time/YMD
    >
    > add goext/time/PrintTime

- 2016/10/01
    > add goext/bitmap
    >
    > optimize goext/time timer by bitmap

- 2016/09/27
    > add uuid in strings
    >
    > add ReadTxt & String & Slice in io/ioutil

- 2016/09/26
    > add CountWatch in goext/time
    >
    > add HostAddress in goext/net
    >
    > add RandString & RanddigitString & UUID for goext/math/rand
    >
    > add Wheel for goext/time

- 2016/09/22
    > add multiple loggers test for goext/log

- 2016/09/21
    > os
    >
    > log

- 2016/08/30
    > sync/trylock
    >
    > sync/semaphore
    >
    > time/time

- 2015/05/03
    > container/xorlist
