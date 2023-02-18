#                                                         Projet Final Spring cloud
# I. Backend :
  * [1. use case](#1-use-case)
  * [2 Inventory service - get all products](#2-inventory-service---get-all-products)
  * [3 Inventory service - get product by ID](#3-inventory-service---get-product-by-id)
  * [4 Customer Service - get All Customers](#4-customer-service---get-all-customers)
  * [5 Customer Service - get Customer by ID](#5-customer-service---get-customer-by-id)
  * [6 Bill Service - get bills](#6-bill-service---get-bills)
  * [7 Bill Service - get bill by id](#7-bill-service---get-bill-by-id)
  * [8 Eureka Service](#8-eureka-service)
# II. Frontend Angular Client :
  * [Login screen](#login-screen)
  ## Products
  * [1 Show all products](#1-show-all-products)
  * [2 Edit products](#2-edit-products)
  * [3 Delete products](#3-delete-products)
  * [4 Search for a product](#4-search-for-a-product)
  * [5 New Product](#5-new-product)
  ## Customers
  * [1 Show all customer](#1-show-all-customer)
  * [2 Edit Customer](#2-edit-customer)
  * [3 Delete Customer](#3-delete-customer)
  * [4 Search Customer](#4-search-customer)
  * [5 New Customer](#5-new-customer)
  ## Bills
  * [Show Bills](#show-bills)
# III Secured Angular Client with keycloak
  * [1 Setup](#1-setup)
  * [2 Postman](#2-postman)
  * [3 Refresh Token](#3-refresh-token)
  * [4 Client Auth Credentials](#4-client-auth-credentials)
# IV. KAFKA :
  * [1 Supplier](#1-supplier)
  * [2 Consumer](#2-Consumer)
  * [3 Kafka streams ](#3-Kafka-streams )
# V. DOCKER :


# I. Backend :
## 1. use case

![image](https://user-images.githubusercontent.com/62290643/206123723-0f5d7345-b23d-4ecb-84cb-83346104a73d.png)

## 2 Inventory service - get all products
```http
GET /localhost:8888/PRODUCT-SERVICE/products
```

![image](https://user-images.githubusercontent.com/55364638/206927699-da2bd3b9-7d1c-4ca3-837e-c9564a826ede.png)



## 3 Inventory service - get product by ID 
```http
GET /localhost:8888/PRODUCT-SERVICE/products/{id}
```

![image](https://user-images.githubusercontent.com/101510983/206922542-ff3bc375-78ba-4956-92f4-0cc57ac762b8.png)



## 4 Customer Service - get All Customers
```http
GET /localhost:8888/CUSTOMER-SERVICE/customers
```

![image](https://user-images.githubusercontent.com/101568947/208621099-0be7dd9c-cc4d-4cc7-a0b8-8623c411291f.png)



## 5 Customer Service - get Customer by ID
```http
GET /localhost:8888/CUSTOMER-SERVICE/customers/{id}
```

![image](https://user-images.githubusercontent.com/101510983/206922673-eb757211-bc70-4d62-a1c2-8248050f3133.png)


## 6 Bill Service - get bills
```http
GET /localhost:8888/BILLING-SERVICE/fullbills
```

![image](https://user-images.githubusercontent.com/101510983/206922042-3f83dfcb-cb8e-4227-ae03-b5013cbf62f5.png)


## 7 Bill Service - get bill by id
```http
GET /localhost:8888/BILLING-SERVICE/fullbills/{id}
```

![image](https://user-images.githubusercontent.com/101510983/206922866-21e9873d-ca97-4434-8cf6-57dfcaadc571.png)


## 8 Eureka Service 
```http
GET /localhost:8761/
```

![image](https://user-images.githubusercontent.com/101510983/206922145-5a50d39f-704c-4ca9-a1cc-ed65cbd20569.png)

# II. Frontend Angular Client :
## Login screen 

![image](https://user-images.githubusercontent.com/101510983/206923607-8e842b94-d208-4dc6-8cbe-fb68db9037ec.png)

## Products
## 1 Show all products 

![image](https://user-images.githubusercontent.com/55364638/206925608-0b8e8b90-f8cf-45af-987a-550ffeb810e3.png)

## 2 Edit products 

![image](https://user-images.githubusercontent.com/101510983/206923795-26a19423-e941-4acd-878e-8a0ce1e3df11.png)

## 3 Delete products 

![image](https://user-images.githubusercontent.com/55364638/206925748-42a715d1-4713-45a3-889a-ccb3ca042ca5.png)

![image](https://user-images.githubusercontent.com/55364638/206925764-23a250e5-8a39-4293-b96c-575a22dfe68b.png)


## 4 Search for a product

![image](https://user-images.githubusercontent.com/55364638/206925802-0b184999-ccb0-4a54-9fc9-dcf419205f83.png)


## 5 New Product

![image](https://user-images.githubusercontent.com/55364638/206925875-db2771c4-e4b0-4970-8f5d-e7c699a9b4ee.png)

![image](https://user-images.githubusercontent.com/55364638/206925899-ebcd1e02-dfbe-43f0-b0ca-e52d264c84a9.png)

## Customers
## 1 Show all customer

![image](https://user-images.githubusercontent.com/101568947/208622005-63e69503-4e87-421b-9ff8-7acdc90bdbc7.png)




## 2 Edit Customer

![image](https://user-images.githubusercontent.com/101568947/208622165-125e76c8-d3df-446d-b092-7be71571018d.png)



## 3 Delete Customer 

![image](https://user-images.githubusercontent.com/101568947/208622330-1a266e93-0b43-44e7-b201-deb64557177a.png)



## 4 Search Customer 

![image](https://user-images.githubusercontent.com/101568947/208622463-5f6fce9c-87bb-42e6-83f5-605ded385b1f.png)



## 5 New Customer 

![image](https://user-images.githubusercontent.com/101568947/208622862-f27bca47-2f38-4b45-a706-090e6bfcd5a0.png)


![image](https://user-images.githubusercontent.com/101568947/208623116-ad28b0c1-d5be-42e6-add7-d17b1ee0e0f6.png)



## Bills
## Show Bills  

![image](https://user-images.githubusercontent.com/101568947/208623424-110cb6ae-f2ce-4942-9509-a8140580087e.png)



# III Secured Angular Client with keycloak 
## 1 Setup
![image](https://user-images.githubusercontent.com/55364638/206931547-ea2548af-8dd5-4a6d-9906-912f9330fe16.png)


## 2 Postman
![image](https://user-images.githubusercontent.com/46407388/206116789-117ba8a2-f337-4fa4-9e01-5d34998c82e5.png)


## 3 Refresh Token
![image](https://user-images.githubusercontent.com/46407388/206116912-121fab06-38fc-4e4b-81ee-cb8013bd2ff1.png)


## 4 Client Auth Credentials
![image](https://user-images.githubusercontent.com/46407388/206117029-7f95a0d9-d7d1-453d-8b0b-8b157098d418.png)



# IV. KAFKA :
## 1. SUPPLIER

![image](https://user-images.githubusercontent.com/73041687/219654966-8ed99356-f63b-449b-b782-480121de32f6.png)

![image](https://user-images.githubusercontent.com/73041687/219687628-a8c20cbe-a87a-4375-9636-b932406c01ac.png)


## 2. CONSUMER

![image](https://user-images.githubusercontent.com/73041687/219677290-25fc52df-3a92-4a65-8e3e-dbf559b2f1de.png)

## 3. KAFKA STREAMS 

![image](https://user-images.githubusercontent.com/73041687/219668084-72ac27e1-849e-4fe5-bec0-9eb63685b965.png)



# V. DOCKER :

## BILLING-SERVICE 
![image](https://user-images.githubusercontent.com/73041687/219697077-feed41f4-1a89-45e8-9206-19451d6dc80a.png)
## INVENTORY-SERVICE
![image](https://user-images.githubusercontent.com/73041687/219698461-5131e970-68b0-4978-a159-a0c628cdb856.png)

![image](https://user-images.githubusercontent.com/73041687/219706814-b5b679ad-30b0-4848-a57c-743ddd0b2798.png)
## CUSTOMER-SERVICE
![1](https://user-images.githubusercontent.com/84048380/219879470-280f20b5-64ba-40ef-b1d1-efbcf85222ba.png)
## GATEWAY-SERVICE
![2](https://user-images.githubusercontent.com/84048380/219879477-5aabf372-83ce-425f-91ce-1b7229f4e183.png)
## EUREKA-DISCOVERY
![3](https://user-images.githubusercontent.com/84048380/219879489-cf5be4ef-21dd-4591-a6fe-729d264c99bc.png)
## FRONTEND
![4](https://user-images.githubusercontent.com/84048380/219879497-61682b3a-19cc-4aba-b753-9088da73a10e.png)
