Тестирование производительности базы данных в PostgreSQL путём создания нагрузки в виде потока транзакций с помощью утилиты pgbench.

Сценарий теста для случая, когда с БД работает один клиент (одна сессия) test_one_connect.py:
1. Инициализация кластера с определённым размером сегмента. Используются размеры сегментов, которые определены в листе wal_segment_size
2. Переопределение параметров в postgresql.conf. Используется для того, чтобы миниимизировать влияние контрольной точки. Праметры checkpoint_timeout и max_wal_size выставляются таким образом, чтобы за время проведения тестов не произошло ни одной контрольной точки
3. Запуск сервера
4. Создание тестовой базы
5. Создание тестовых таблиц и проведение нагрузочного тестирования с помощью утилиты pgbench в PostgreSQL. Перед проведением теста принудительно совершается контрольная точка.
6. Остановка сервера

Данный тест использовался для проверки, имеется ли в принципе какое-то влияние от изменения размера сегмента. Влияние было подтверждено.
Для исследования влияния размера сегмента на производительность при нескольких активных подключениях был реализован test_max_connect.py. Данный тест проводится по аналогии со сценарием описанным выше, только при вызове утилиты pgbench ключом -c задается необходимое количество подключений.
Также представлен парсер, который считывает значения tps из текстовых файлов, которые были записаны в результате запуска тестов. По значениям tps строятся графики изменения показателей производительности в зависимости от размера сегмента WAL.

Использовать проект можно на операционной системе ubuntu 22.04. Также должен быть установлен python. Чтобы провести тесты необходимо запустить скрипт от root, либо раздать доступ на внесение изменений без требования пароля пользователю в sudoers. 
