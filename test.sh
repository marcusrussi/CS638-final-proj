cd src;
make -j;
cd ..
for i in `seq 1 11`
do
    echo "m = " $i >> results_m.txt
    timeout 90s bin/deployment/db 8 m $i | tail -60 >> results_m.txt
    echo ""
done
