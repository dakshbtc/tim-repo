ps -ef | grep main_equities.py | grep -v grep | awk '{print $2}' | xargs kill
ps -ef | grep update_equities.py | grep -v grep | awk '{print $2}' | xargs kill

nohup python3 -u main_equities.py > logs/main_equities.out &
nohup python3 -u update_equities.py > logs/update_equities.out &