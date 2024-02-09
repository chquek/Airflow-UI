set -x

python3 app.py &
# python3 app_ssl.py &

wait -n

exit $?
