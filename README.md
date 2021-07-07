# AsyncSettled

## 使用方法


### 初始化 (兩種方法選一種)
1. 實體化物件
```php
use GiocoPlus\AsyncSettled\AsyncSettled;
```

```php
/**
 * AsyncSettled constructor.
 * @param string $opCode 營商代碼
 * @param string $vendorCode 遊戲商代碼
 * @param string $gameCode 遊戲代碼
 * @param string $parentBetId 父注編號
 * @param string $betId 下注編號
 * @param array $member 會員資料 (key 需 player_name & member_code)
 */
 
$asyncSettled = new AsyncSettled($opCode, $vendorCode, $gameCode, $parentBetId, $betId, $member);
```

2. 注入懶載入代理
```php
use GiocoPlus\AsyncSettled\AsyncSettled;
```
```php
/**
 * @Inject(lazy=true)
 * @var AsyncSettled
 */
private $asyncSettled;
```
```php
$this->asyncSettled->setDefault($opCode, $vendorCode, $gameCode, $parentBetId, $betId, $member);
```

### 使用方法
待寫