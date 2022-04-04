set -e

for driver in scylla-go-driver gocql rust; do
    echo "Building $driver..."
    cd $driver
    ./build.sh
    cd ..
done

printf "\nDONE\n"
