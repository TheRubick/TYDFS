declare -i N=0
#echo $N
N=$(($1))
N=$(($N - 1))
#echo $N
declare -i dkNum=0
for x in $( eval echo {0..$N})
do
    rm -r dk${dkNum}Dir &
    dkNum+=1
done
