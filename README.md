# PAD
___
## lw3
Проект расположен на сервере ITMO. Для подключения писать [мне](t.me/chernovec).

После подключения к проекту выполняем следующие шаги:
1. Запускаем сессию screen для JupyterLab, чтобы соединение с сервером не обрывалось:
  - на сервере: screen -S jupyter -p 8889
  - переходим в каталог lw3
  - подключаем вирт. окр.: source venv\bin\activate
  - берем токен подключения из вывода
  - на локалке: ssh -N -L <local_port>:localhost:<session_port> -p <server_port> <login>@<server_ip>
  - переходим по ссылке: http://localhost:<local_port>
2. Запускаем сессию screen для Airflow:
  - на сервере: screen -S airflow
  - переходим в каталог lw3
  - подключаем вирт. окр.: source venv\bin\activate
  - запускаем контейнер: sudo docker-compose up
  - на локалке: ssh -N -L <local_port>:localhost:<session_port> -p <server_port> <login>@<server_ip>
  - переходим по ссылке: http://localhost:<local_port>

Теперь у нас есть доступ к Notebook и Airflow.
