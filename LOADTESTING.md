# Load testing

## TEST

### PUT 2/3

https://overload.yandex.net/148256

### GET 2/3

https://overload.yandex.net/148265

### GET PUT 2/3

https://overload.yandex.net/148270

### PUT 3/3

https://overload.yandex.net/148266

### GET 3/3

https://overload.yandex.net/148267

### GET PUT 3/3

https://overload.yandex.net/148272

## Profiling JMC

https://drive.google.com/open?id=1ujb4DzxelEGWzaJeZiDisIpcbBBjkIq9

## OPTIMIZE

После оптимизации удалось значительно улучшить 'PUT' за счет увеличения временного хранилища, увеличения размера B+Tree,
 оптимизации GC. В 'GET' замечен лишь небольшой прирост.
 
### PUT 3/3
https://overload.yandex.net/148589
 