SIZES=(10 100 1000 10000 100000)
PLAYERS=(1 2 3)

for SIZE in ${SIZES[@]}; do
    for PLAYER in ${PLAYERS[@]}; do
        python3 ../../gen_util.py /mnt/shared/$SIZE/in$PLAYER.csv 2 $SIZE "a,b"
    done
done
