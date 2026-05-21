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

In your newly cloned `dapr-workshop-python` repository, navigate to the `/resources` folder and create a new file called `statestore.yaml`. Add the content below to the file:

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

Now navigate to the `/pizza-order` directory. This folder contains all the files you need for your first service. Before beginning to code, install the Dapr dependencies by running the following in a new terminal window:

```bash
# Create virtual environment
python3 -m venv env
source env/bin/activate

# Navigate to the service folder and add the Dapr package
cd pizza-order
pip install -r requirements.txt
```

## Create the service

Open `/pizza-order/app.py`. Import the `DaprClient` library by including the line below:

```python
from dapr.clients import DaprClient
```

This will import _DaprClient_ from _dapr.clients_. That is what you will use to manage the state in the Redis instance.

## Manage state

This file has 4 routes:

- `POST /order-sub`: used to subscribe to pub/sub messages. We will cover this on Challenge 3.
- `POST /order`: creates a new order.
- `GET /order/<order_id>`: gets an order by id.
- `DELETE /order/<order_id>`: deletes an order by id.

In this chapter we will focus on creating, retrieving, and deleting an order.

1. Start by creating a const that refers to the Dapr state store component defined in the previous step. This name **must** be the same as the `metadata.name` in the Dapr component spec.

```python
DAPR_STORE_NAME = 'pizzastatestore'
```

2. `create_order` creates or updates an order. Update the `TODO:` section with the following code:

```python
# Save order state
with DaprClient() as client:
    client.save_state(
        store_name=DAPR_STORE_NAME,
        key=f"order_{order_id}",
        value=json.dumps(order_data)
    )
    
    logger.info(f"Created order {order_id}")
```

3. `get_order` retrieves an order from the state store. Update the `TODO:` section with the code below:

```python
with DaprClient() as client:
    state = client.get_state(
        store_name=DAPR_STORE_NAME,
        key=f"order_{order_id}"
    )
    
    if not state.data:
        return jsonify({'error': 'Order not found'}), 404
        
    order_data = json.loads(state.data)
    return jsonify(order_data)
```

4. `delete_order` deletes the order from the state store. Update the `TODO:` section with the code below:

```python
with DaprClient() as client:
    client.delete_state(
        store_name=DAPR_STORE_NAME,
        key=f"order_{order_id}"
    )
    
    return jsonify({'success': True})
```


The Dapr Client is responsible for the following, respectively:

1. `client.save_state(store_name=DAPR_STORE_NAME key=f"order_{order_id}", value=json.dumps(order_data))` saves the state the Redis using a key/value pair. It requires the state store name, the order id as a **key**, and a json representation of the order as a **value**.

2. `result = client.get_state(store_name=DAPR_STORE_NAME key=f"order_{order_id}")` retrieves the state from the store. It requires a key and the state store name.

3. `client.delete_state(store_name=DAPR_STORE_NAME key=f"order_{order_id}")` deletes the state from the store. It also requires a key and the state store name.

## Run the application

Now, open a terminal and navigate to the `/pizza-order` folder where `app.py` is located. Use the Dapr CLI to run the following command:

```bash
dapr run --app-id pizza-order --app-protocol http --app-port 8001 --dapr-http-port 3501 --resources-path ../resources  -- python3 app.py
```

> [!IMPORTANT]
> If you are using Consul as a naming resolution service, add `--config ../resources/config/config.yaml` before `-- python3 app.py` on your Dapr run command.

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
    "orderId": "123",
    "pizzaType": "pepperoni",
    "size": "large",
    "customer": {
        "name": "John Doe",
        "address": "123 Main St",
        "phone": "555-0123"
    }
}
```

Run the `GET` and `DELETE` requests situated below to get and delete the order as well.

#### Use _cURL_

Run the command below to create a new order.

```bash
curl -H 'Content-Type: application/json' \
    -d '{ "orderId": "123", "pizzaType": "pepperoni", "size": "large", "customer": { "name": "John Doe", "address": "123 Main St", "phone": "555-0123" } }' \
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

Create a new service to create the order, cook, and deliver the pizza. In the next challenge, you will learn how to create a new API endpoint and how to invoke it using Dapr. When you are ready, go to Challenge 2: [Service Invocation](/docs/challenge-2/python.md)!
