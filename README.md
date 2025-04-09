# Distributed checkout

### How to run the application

run  `docker compose up --scale order-service=4 --scale stock-service=4 --scale payment-service=4 --build -d` to build the app with 4 instances of each service.

### Consistency
Consistency is achieved with the SAGA microservice pattern. Firstly we try to do the payment and then subtract the stock (if the payment was successful). In case the payment goes through, but we find that there isn't enough stock, we apply the compensating transaction for the payment service. To deal with duplicate events in the system we also use idempotency keys for the operations performed by `payment` and `stock` services. The story gets more complicated when we consider possible failures in this distributed transaction.
### Fault tolerance
We have identified many failure points in the system. Firstly, consider that each service's database might fail. This was solved by having replicas for the DBs (using Redis Sentinel to take care of replication and fail-over switch). Secondly, some services might fail. If one of the 4 `order` instances fail, the instance that fails uses undo logging to recover when it restarts. If any of the `stock` and `payment` instances fail kafka redistributes its topics to the other running instances.

### Undo logging
We use undo logging in the `order` service. We write in the log when we start the checkout and write again that the order is completed when the respective saga is finished. In recovery, we go through uncompleted orders. The `stock` and `payment` DBs help us reconstruct what happened with a given saga (if it was completed in both, if it was rolledback in some service but not the other, etc.) and we can change the status of the order in our DB to make sure we stay consistent.

### Scalability
Scalability of the system is ensured by many key elements, such as asynchronous processing via a publisher-subscriber pattern and smart partitioning of kafka topics. Using kafka, each `order` service instance maintains its own topic to handle the saga's that they are responsible for. There is a single topic for sending tasks to `stock` and a single topic for sending tasks to `payment`. We will have as many partitions for this topic as we have consumers to ensure even work distribution. 

### Project structure

* `env`
    Folder containing the Redis env variables for the docker-compose deployment
    
* `helm-config` 
   Helm chart values for Redis and ingress-nginx
        
* `k8s`
    Folder containing the kubernetes deployments, apps and services for the ingress, order, payment and stock services.
    
* `order`
    Folder containing the order application logic and dockerfile. 
    
* `payment`
    Folder containing the payment application logic and dockerfile. 

* `stock`
    Folder containing the stock application logic and dockerfile. 

* `test`
    Folder containing some basic correctness tests for the entire system. (Feel free to enhance them)

### Deployment types:

#### docker-compose (local development)

After coding the REST endpoint logic run `docker-compose up --build` in the base folder to test if your logic is correct
(you can use the provided tests in the `\test` folder and change them as you wish). 

***Requirements:*** You need to have docker and docker-compose installed on your machine. 

K8s is also possible, but we do not require it as part of your submission. 

#### minikube (local k8s cluster)

This setup is for local k8s testing to see if your k8s config works before deploying to the cloud. 
First deploy your database using helm by running the `deploy-charts-minicube.sh` file (in this example the DB is Redis 
but you can find any database you want in https://artifacthub.io/ and adapt the script). Then adapt the k8s configuration files in the
`\k8s` folder to mach your system and then run `kubectl apply -f .` in the k8s folder. 

***Requirements:*** You need to have minikube (with ingress enabled) and helm installed on your machine.

#### kubernetes cluster (managed k8s cluster in the cloud)

Similarly to the `minikube` deployment but run the `deploy-charts-cluster.sh` in the helm step to also install an ingress to the cluster. 

***Requirements:*** You need to have access to kubectl of a k8s cluster.
