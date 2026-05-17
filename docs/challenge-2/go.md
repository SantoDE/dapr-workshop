# Challenge 2 - Service Invocation

## Overview

In this challenge, you will create three services that will change the status of your order via service invocations. For that, you will:

- Create a new service called `pizza-storefront` with a `/order` endpoint.
- Create a new service called `pizza-kitchen` with a `/cook` endpoint.
- Create a new service called `pizza-delivery` with a `/deliver` endpoint.
- Use Dapr's Service Invocation API to call the `/cook` and `/deliver` endpoints from the `pizza-storefront` app.

<img src="../../imgs/challenge-2.png" width=45%>

To learn more about the Dapr Service Invocation building block, refer to the [Dapr docs](https://docs.dapr.io/developing-applications/building-blocks/service-invocation/).

## Install the dependencies

Navigate to the root of your project and install dependencies for all services:

```bash
make tidy
```

Or install them individually:

```bash
cd pizza-storefront && go mod tidy
cd ../pizza-kitchen && go mod tidy
cd ../pizza-delivery && go mod tidy
```

## Create the service

Both files `/pizza-kitchen/main.go` and `/pizza-delivery/main.go` already contain the endpoint handlers that will be invoked from the `pizza-storefront` service. No changes are needed there for this challenge.

Navigate to the `/pizza-storefront` service folder. Open `main.go`.

## Add service invocation

In Go, Dapr service invocation over HTTP works by sending requests to the local Dapr sidecar with a `dapr-app-id` header. The sidecar handles service discovery and forwards the call to the target service.

Inside the `processOrder` function, replace the `// TODO: Add Service invocation code` comment with the following:

```go
// Call pizza-kitchen via Dapr service invocation
if err := invokeService("pizza-kitchen", "cook", orderData); err != nil {
    return fmt.Errorf("failed to call pizza-kitchen: %w", err)
}

// Call pizza-delivery via Dapr service invocation
if err := invokeService("pizza-delivery", "deliver", orderData); err != nil {
    return fmt.Errorf("failed to call pizza-delivery: %w", err)
}
```

Add the `invokeService` helper function to the file:

```go
const daprHTTPPort = 3502

// invokeService calls another Dapr app through the local sidecar HTTP proxy
func invokeService(appID, method string, payload any) error {
    body, err := json.Marshal(payload)
    if err != nil {
        return err
    }

    targetURL := fmt.Sprintf("http://localhost:%d/%s", daprHTTPPort, method)
    req, err := http.NewRequest(http.MethodPost, targetURL, bytes.NewReader(body))
    if err != nil {
        return err
    }
    req.Header.Set("dapr-app-id", appID)
    req.Header.Set("Content-Type", "application/json")

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()

    log.Printf("Service invocation to %s/%s: status %d", appID, method, resp.StatusCode)
    return nil
}
```

Don't forget to add `"bytes"` to your import block.

### Breaking down the code above

1. First the request is built targeting the **local Dapr sidecar** port (`3502`), not the remote service directly:

```go
targetURL := fmt.Sprintf("http://localhost:%d/%s", daprHTTPPort, method)
```

2. The `dapr-app-id` header tells the Dapr sidecar which application to route the request to:

```go
req.Header.Set("dapr-app-id", appID)
```

The Dapr sidecar of the `pizza-storefront` application uses this header to discover and invoke the target service. This way, services only need to communicate to their associated sidecar over localhost and the sidecar handles service discovery and invocation:

<img src="../../imgs/service-invocation.png" width=60%>

The same process is applied to invoke the `/deliver` endpoint on the `pizza-delivery` service.

## Run the applications

It's now time to run all three applications.

1. In your terminal, ensure you are in the `/pizza-storefront` folder and run the command below:

```bash
dapr run --app-id pizza-storefront --app-protocol http --app-port 8002 --dapr-http-port 3502 -- go run .
```

2. Open a new terminal window and navigate to `/pizza-kitchen`. Run the command below:

```bash
dapr run --app-id pizza-kitchen --app-protocol http --app-port 8003 --dapr-http-port 3503 -- go run .
```

3. Open a third terminal window and navigate to `/pizza-delivery`. Run the command below:

```bash
dapr run --app-id pizza-delivery --app-protocol http --app-port 8004 --dapr-http-port 3504 -- go run .
```

> [!IMPORTANT]
> If you are using Consul as a naming resolution service, add `--config ../resources/config/config.yaml` before `-- go run .` on your Dapr run command.

## Test the service

### Use VS Code REST Client

Open `Endpoints.http` and find the `Direct Pizza Store Endpoint (for testing)` endpoint call. Click on `Send request`.

Navigate to the `pizza-storefront` terminal, where you should see the following logs:

```zsh
== APP == 2024/12/05 22:40:42 Received new order: 123
== APP == 2024/12/05 22:40:42 Order 123 - validating
== APP == 2024/12/05 22:40:43 Order 123 - processing
== APP == 2024/12/05 22:40:45 Order 123 - confirmed
== APP == 2024/12/05 22:40:45 Service invocation to pizza-kitchen/cook: status 200
== APP == 2024/12/05 22:40:58 Service invocation to pizza-delivery/deliver: status 200
```

The logs for `pizza-kitchen` should read:

```zsh
== APP == 2024/12/05 22:40:45 Starting cooking for order: 123
== APP == 2024/12/05 22:40:45 Order 123 - preparing_ingredients
== APP == 2024/12/05 22:40:47 Order 123 - making_dough
== APP == 2024/12/05 22:40:50 Order 123 - adding_toppings
== APP == 2024/12/05 22:40:52 Order 123 - baking
== APP == 2024/12/05 22:40:57 Order 123 - quality_check
== APP == 2024/12/05 22:40:58 Order 123 - cooking completed
```

Finally, on `pizza-delivery`:

```zsh
== APP == 2024/12/05 22:40:58 Starting delivery for order: 123
== APP == 2024/12/05 22:40:58 Order 123 - finding_driver
== APP == 2024/12/05 22:41:00 Order 123 - driver_assigned
== APP == 2024/12/05 22:41:01 Order 123 - picked_up
== APP == 2024/12/05 22:41:03 Order 123 - on_the_way
== APP == 2024/12/05 22:41:08 Order 123 - arriving
== APP == 2024/12/05 22:41:10 Order 123 - at_location
== APP == 2024/12/05 22:41:11 Order 123 - delivery completed
```

### Use _cURL_

Alternatively, open a new terminal window and create a new order via cURL:

```bash
curl -H 'Content-Type: application/json' \
    -d '{ "order_id": "123", "pizza_type": "pepperoni", "size": "large", "customer": { "name": "John Doe", "address": "123 Main St", "phone": "555-0123" } }' \
    -X POST \
    http://localhost:8002/order
```

## Next Steps

Now that the services are updating the event information for every order step, you need to make sure that this information is being updated in the Redis state store. You will do this in the next challenge using Dapr [Pub/Sub](/docs/challenge-3/go.md)!
