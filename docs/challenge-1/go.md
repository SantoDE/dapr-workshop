# Challenge 1 - State Store

## Overview

> Ensure you have completed the [technical prerequisites](../prerequisites.md) before starting the challenges.

In this challenge, you will:

- Configure a State Store component using a local Redis instance to save, get, and delete a pizza order.
- Update the `pizza-order` application to use the Dapr State Management API.
- Run the app locally using the Dapr CLI.

<img src="../../imgs/challenge-1.png" width=50%>

To learn more about the Dapr State Management Building Block, refer to the [Dapr docs](https://docs.dapr.io/developing-applications/building-blocks/state-management/state-management-overview/).

## Configure the state store

In your newly cloned `dapr-workshop-go` repository, navigate to the `/resources` folder and create a new file called `statestore.yaml`. Add the content below to the file:

```yaml
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: pizzastatestore
spec:
  type: state.redis
  version: v1
  metadata:
  - name: redisHost
    value: localhost:6379
  - name: redisPassword
    value: ""
```

This is a Dapr Component specification file named `pizzastatestore`. In the _spec_ definition, note that the type of the component is `state.redis` and the metadata contains host and password information for the local Redis instance that was deployed as a container during Dapr's initialization process.

## Install dependencies

Navigate to the `/pizza-order` directory and install the Go module dependencies:

```bash
cd pizza-order
go mod tidy
```

Or from the root of the repository, use the Makefile to install dependencies for all services at once:

```bash
make tidy
```

## Create the service

Open `/pizza-order/main.go`. Import the Dapr Go SDK client by adding the following import:

```go
import (
    dapr "github.com/dapr/go-sdk/client"
)
```

## Manage state

This file has 4 routes:

- `POST /orders-sub`: used to subscribe to pub/sub messages. We will cover this in Challenge 3.
- `POST /order`: creates a new order.
- `GET /order/{order_id}`: gets an order by id.
- `DELETE /order/{order_id}`: deletes an order by id.

In this chapter we will focus on creating, retrieving, and deleting an order.

1. Start by declaring a constant for the Dapr state store component name. This name **must** be the same as the `metadata.name` in the Dapr component spec:

```go
const daprStoreName = "pizzastatestore"
```

2. Declare a package-level Dapr client variable and initialize it in `main()`:

```go
var daprClient dapr.Client

func main() {
    var err error
    daprClient, err = dapr.NewClient()
    if err != nil {
        log.Fatalf("Failed to create Dapr client: %v", err)
    }
    defer daprClient.Close()

    // ... rest of main
}
```

3. `createOrder` saves a new order to the state store. Replace the `// TODO: Save the order data to the state store` comment with:

```go
orderJSON, err := json.Marshal(orderData)
if err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}

stateKey := fmt.Sprintf("order_%s", orderData.OrderID)
if err := daprClient.SaveState(context.Background(), daprStoreName, stateKey, orderJSON, nil); err != nil {
    log.Printf("Error creating order %s: %v", orderData.OrderID, err)
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}

log.Printf("Created order %s", orderData.OrderID)
```

4. `getOrder` retrieves an order from the state store. Replace the `// TODO: Get the order data from the state store` comment with:

```go
stateKey := fmt.Sprintf("order_%s", orderID)
state, err := daprClient.GetState(context.Background(), daprStoreName, stateKey, nil)
if err != nil {
    log.Printf("Error getting order %s: %v", orderID, err)
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}

if len(state.Value) == 0 {
    http.Error(w, `{"error":"Order not found"}`, http.StatusNotFound)
    return
}

var orderData OrderData
if err := json.Unmarshal(state.Value, &orderData); err != nil {
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(orderData)
```

5. `deleteOrder` deletes the order from the state store. Replace the `// TODO: Delete the order data from the state store` comment with:

```go
stateKey := fmt.Sprintf("order_%s", orderID)
if err := daprClient.DeleteState(context.Background(), daprStoreName, stateKey, nil); err != nil {
    log.Printf("Error deleting order %s: %v", orderID, err)
    http.Error(w, err.Error(), http.StatusInternalServerError)
    return
}

w.Header().Set("Content-Type", "application/json")
json.NewEncoder(w).Encode(map[string]bool{"success": true})
```

The Dapr client is responsible for the following, respectively:

1. `daprClient.SaveState(ctx, daprStoreName, stateKey, data, nil)` saves the state to Redis using a key/value pair. It requires the state store name, the order id as a **key**, and a JSON-encoded order as a **value**.

2. `daprClient.GetState(ctx, daprStoreName, stateKey, nil)` retrieves the state from the store. The result's `Value` field contains the raw bytes.

3. `daprClient.DeleteState(ctx, daprStoreName, stateKey, nil)` deletes the state from the store.

## Run the application

Open a terminal and navigate to the `/pizza-order` folder. Use the Dapr CLI to run the following command:

```bash
dapr run --app-id pizza-order --app-protocol http --app-port 8001 --dapr-http-port 3501 --resources-path ../resources -- go run .
```

> [!IMPORTANT]
> If you are using Consul as a naming resolution service, add `--config ../resources/config/config.yaml` before `-- go run .` on your Dapr run command.

This command sets:
  - the app-id as `pizza-order`
  - the app-protocol to `http`
  - an app-port of `8001` for Dapr communication into the app
  - an http-port of `3501` for Dapr API communication from the app
  - the resources-path, where the state store component definition file is located. This will guarantee that the Redis component is loaded when the app initializes.

Look for the log entry below to guarantee that the state store component was loaded successfully:

```bash
INFO[0000] Component loaded: pizzastatestore (state.redis/v1)  app_id=pizza-order instance=diagrid.local scope=dapr.runtime.processor type=log ver=1.14.4
```

## Test the service

### Use VS Code REST Client

Open the `Endpoints.http` file located in the root of the repository and place a new order by clicking the button `Send request` under `Direct Pizza Order Endpoint (for testing)`:

![send-request](/imgs/rest-request.png)

```http
### Direct Pizza Order Endpoint (for testing)
POST {{pizzaOrderUrl}}/order
Content-Type: application/json

{
    "order_id": "123",
    "pizza_type": "pepperoni",
    "size": "large",
    "customer": {
        "name": "John Doe",
        "address": "123 Main St",
        "phone": "555-0123"
    }
}
```

Run the `GET` and `DELETE` requests situated below to get and delete the order as well.

### Use _cURL_

Run the command below to create a new order:

```bash
curl -H 'Content-Type: application/json' \
    -d '{ "order_id": "123", "pizza_type": "pepperoni", "size": "large", "customer": { "name": "John Doe", "address": "123 Main St", "phone": "555-0123" } }' \
    -X POST \
    http://localhost:8001/order
```

Get:

```bash
curl -H 'Content-Type: application/json' \
    -X GET \
    http://localhost:8001/order/123
```

Finally, delete the order:

```bash
curl -H 'Content-Type: application/json' \
    -X DELETE \
    http://localhost:8001/order/123
```

### Visualize the data

If you downloaded Redis Insight, you can visualize the new order there:

![redis-insight](/imgs/redis-insight.png)

## Next steps

Create a new service to create the order, cook, and deliver the pizza. In the next challenge, you will learn how to create a new API endpoint and how to invoke it using Dapr. When you are ready, go to Challenge 2: [Service Invocation](/docs/challenge-2/go.md)!
