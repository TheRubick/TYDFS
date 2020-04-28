
declare -i N=0
N=$1
declare -i dkNumProcess=$2
declare -i replicateNum=$3
python3 Master.py $1 $2 $3 &
#echo $N
N=$(($1))
N=$(($N - 1))
#echo $N
declare -i dkNum=0
for x in $( eval echo {0..$N})
do
    python3 DataKeeper.py $dkNum 1 &
    mkdir dk${dkNum}Dir &
    dkNum+=1
done
#python3 DataKeeper.py 1 1 &
#python3 DataKeeper.py 2 1 &
#python3 DataKeeper.py 3 1 &
#mkdir dk0Dir &
#mkdir dk1Dir &
#mkdir dk2Dir &
#mkdir dk3Dir &
