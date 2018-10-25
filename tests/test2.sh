for i in $( seq 1000 )
do
	key=$(( $i * 3141592654 % 1000000 ))
	val=$(( $i * 2718281828459045 ))
	echo "+${#key},${#val}:${key}->${val}"
	echo "+3,${#i}:one->${i}"
done
echo "+3,7:two->Goodbye"
echo
