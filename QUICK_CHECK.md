# Быстрая проверка системы

## Запуск

```bash
docker compose up --build
```

## 1. Auth Service

### Регистрация SELLER
```bash
curl -X POST http://localhost/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"seller1","email":"seller@test.com","password":"pass123","role":"SELLER"}'
```

### Логин
```bash
curl -X POST http://localhost/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"seller@test.com","password":"pass123"}'
```

Сохраните `access_token` из ответа.

### Проверка токена
```bash
TOKEN="ваш_токен"
curl http://localhost/auth/me \
  -H "Authorization: Bearer $TOKEN"
```

### Проверка в БД
```bash
docker compose exec auth-db psql -U auth_user -d auth_db \
  -c "SELECT username, email, role FROM users;"
```

## 2. Product Service

### Создать товар (нужен токен SELLER)
```bash
curl -X POST http://localhost/products \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"Laptop","description":"Gaming laptop","price":1500.00,"stock":10,"category":"Electronics","status":"ACTIVE"}'
```

### Список товаров
```bash
curl http://localhost/products
```

### Список с фильтрацией
```bash
curl "http://localhost/products?status=ACTIVE&category=Electronics&page=0&size=10"
```

### Получить товар
```bash
PRODUCT_ID="id_из_предыдущего_ответа"
curl http://localhost/products/$PRODUCT_ID
```

### Обновить товар
```bash
curl -X PUT http://localhost/products/$PRODUCT_ID \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"price":1400.00,"stock":15}'
```

### Удалить товар (мягкое удаление)
```bash
curl -X DELETE http://localhost/products/$PRODUCT_ID \
  -H "Authorization: Bearer $TOKEN"
```

### Проверка в БД
```bash
docker compose exec product-db psql -U product_user -d product_db \
  -c "SELECT id, name, price, stock, status, seller_id FROM products;"
```

## 3. Проверка RBAC

### Регистрация USER
```bash
curl -X POST http://localhost/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"user1","email":"user@test.com","password":"pass123","role":"USER"}'
```

### Логин USER
```bash
curl -X POST http://localhost/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"user@test.com","password":"pass123"}'
```

### Попытка создать товар (должна вернуть 403)
```bash
USER_TOKEN="токен_user"
curl -X POST http://localhost/products \
  -H "Authorization: Bearer $USER_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"Test","price":100,"stock":5,"category":"Test"}'
```

Ожидаемый ответ:
```json
{
  "error_code": "ACCESS_DENIED",
  "message": "Only SELLER and ADMIN can create products"
}
```

## 4. Проверка валидации

### Невалидная цена
```bash
curl -X POST http://localhost/products \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name":"Test","price":-100,"stock":5,"category":"Test"}'
```

Ожидаемый ответ:
```json
{
  "error_code": "VALIDATION_ERROR",
  "message": "Validation failed",
  "details": {
    "price": "Price must be greater than 0"
  }
}
```

## 5. Проверка логирования

Логи в JSON формате с `request_id`:

```bash
docker compose logs auth-service | grep request_id
docker compose logs product-service | grep request_id
```

## 6. OpenAPI спецификации

```bash
cat auth-service/openapi/auth-api.yaml
cat product-service/openapi/product-api.yaml
```
