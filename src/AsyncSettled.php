<?php

declare(strict_types=1);
namespace GiocoPlus\AsyncSettled;

use Carbon\Carbon;
use Exception;
use GiocoPlus\Mongodb\MongoDb;
use GiocoPlus\PrismConst\Constant\TransactionConst;
use GiocoPlus\PrismConst\State\TransactionState;
use GiocoPlus\PrismConst\Tool\ApiResponse;
use GiocoPlus\PrismPlus\Repository\DbManager;
use GiocoPlus\PrismPlus\Service\OperatorCacheService;
use Hyperf\Utils\ApplicationContext;
use MongoDB\BSON\UTCDateTime;

class AsyncSettled
{
    /**
     * @var DbManager
     */
    private $dbManager;

    /**
     * @var OperatorCacheService
     */
    private $opCache;

    /**
     * @var string
     */
    private $asyncSettledCol;

    /**
     * @var string
     */
    private $precountFixCol;

    /**
     * @var MongoDb
     */
    protected $mongodb;

    /**
     * @var string
     */
    private $mongoDefaultPool = 'default';

    /**
     * @var string
     */
    protected $carbonTimeZone = "Asia/Taipei";

    /**
     * @var string 營商代碼
     */
    protected $opCode;

    /**
     * @var string 遊戲商代碼
     */
    protected $vendorCode;

    /**
     * @var string 遊戲代碼
     */
    protected $gameCode;

    /**
     * @var string 父注編號
     */
    protected $parentBetId;

    /**
     * @var string 下注編號
     */
    protected $betId;

    /**
     * @var array 會員資料 (key 需 player_name & member_code)
     */
    protected $member;

    /**
     * 幣值轉換值
     * @var float
     */
    protected $currencyRate;

    /**
     * 錢包精度
     * @var int
     */
    protected $currencyScale = 4;

    /**
     * AsyncSettled constructor.
     * @param string $opCode 營商代碼
     * @param string $vendorCode 遊戲商代碼
     * @param string $gameCode 遊戲代碼
     * @param string $parentBetId 父注編號
     * @param string $betId 下注編號
     * @param array $member 會員資料 (key 需 player_name & member_code)
     */
    public function __construct(string $opCode = '', string $vendorCode = '', string $gameCode = '', string $parentBetId = '', string $betId = '', array $member = [])
    {
        $this->dbManager = ApplicationContext::getContainer()->get(DbManager::class);
        $this->mongodb = ApplicationContext::getContainer()->get(MongoDb::class);
        $this->opCache = ApplicationContext::getContainer()->get(OperatorCacheService::class);

        $this->asyncSettledCol = "async_settled";
        $this->precountFixCol = "precount_fix";

        $this->opCode = $opCode;
        $this->vendorCode = $vendorCode;
        $this->gameCode = $gameCode;
        $this->parentBetId = $parentBetId;
        $this->betId = $betId;
        $this->member = $member;

        // 幣別換算配置
        $currencyRates = $this->opCache->currencyRate($opCode);
        if (isset($currencyRates[$vendorCode]) === false) {
            return ApiResponse::result([], TransactionState::TRANS_CURRENCY_RATE_EMPTY);
        }
        $this->currencyRate = $currencyRates[$vendorCode];
    }

    /**
     * @param string $opCode 營商代碼
     * @param string $vendorCode 遊戲商代碼
     * @param string $gameCode 遊戲代碼
     * @param string $parentBetId 父注編號
     * @param string $betId 下注編號
     * @param array $member 會員資料 (key 需 player_name & member_code)
     * @throws Exception
     */
    public function setDefault(string $opCode, string $vendorCode, string $gameCode, string $parentBetId, string $betId, array $member): AsyncSettled
    {
        $this->opCode = $opCode;
        $this->vendorCode = $vendorCode;
        $this->gameCode = $gameCode;
        $this->parentBetId = $parentBetId;
        $this->betId = $betId;
        $this->member = $member;

        // 幣別換算配置
        $currencyRates = $this->opCache->currencyRate($opCode);
        if (isset($currencyRates[$vendorCode]) === false) {
            throw new \Exception(TransactionState::TRANS_CURRENCY_RATE_EMPTY['msg']);
        }
        $this->currencyRate = $currencyRates[$vendorCode];
        return $this;
    }

    /**
     * 標記為 下注 (未結算)
     * @param float $stakeAmount 下注金額
     * @param int $stakeTime 下注時間
     * @return bool
     * @throws Exception
     */
    public function stake(float $stakeAmount, int $stakeTime)
    {
        try {
            $playerName = $this->member["player_name"];
            $memberCode = $this->member["member_code"];

            $stakeTime = $this->toTime13($stakeTime);

            if (empty($this->asyncSettledLog($this->opCode, $this->vendorCode, $playerName, $this->parentBetId, $this->betId))) {
                $record = [
                    "vendor_code" => $this->vendorCode,
                    "game_code" => $this->gameCode,
                    "parent_bet_id" => $this->parentBetId,
                    "bet_id" => $this->betId,
                    "player_name" => $playerName,
                    "member_code" => $memberCode,
                    "bet_amount" => $this->exchangeRate($stakeAmount, '/'),
                    "win_amount" => 0,
                    "vendor_bet_amount" => $stakeAmount,
                    "vendor_win_amount" => 0,
                    "bet_time" => $stakeTime,
                    "settled_time" => 0,
                    "status" => TransactionConst::STAKE,
                    "total" => 1,
                    "created_at" => new UTCDateTime(),
                    "updated_at" => new UTCDateTime(),
                    "deleted_at" => "", # ttl 刪除用 (結算時會標記上時間，其餘該欄位值設為 null)
                ];

                $result = $this->dbManager->opMongoDb($this->opCode)->insert($this->asyncSettledCol, $record);
                if ($result !== false) {
                    return true;
                }
            }
        } catch (\Throwable $th) {
            throw new Exception($th->getMessage());
        }
        return false;
    }

    /**
     * 標記為 派彩 (結算)
     * @param float $stakeAmount 下注金額
     * @param int $stakeTime 下注時間
     * @param float $payoffAmount 結算金額
     * @param int $payoffTime 結算時間
     * @param int $total 注單數量
     *
     * @return bool
     * @throws Exception
     */
    public function payoff(float $stakeAmount, int $stakeTime, float $payoffAmount, int $payoffTime, int $total)
    {
        try {
            $hasCreateStake = $this->stake($stakeAmount, $stakeTime);

            // $stakeTime = $this->toTime13($stakeTime);
            $payoffTime = $this->toTime13($payoffTime);
            $updateTime = $payoffTime;

            $playerName = $this->member['player_name'];
            // $memberCode = $this->member["member_code"];

            $asyncSettledLog = $this->asyncSettledLog($this->opCode, $this->vendorCode, $playerName, $this->parentBetId, $this->betId);
            if (!empty($asyncSettledLog)) {
                if ($hasCreateStake || ($updateTime > $asyncSettledLog['settled_time'])) {
                    $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->asyncSettledCol, [
                        "vendor_code" => $this->vendorCode,
                        "player_name" => $playerName,
                        "parent_bet_id" => $this->parentBetId,
                        "bet_id" => $this->betId,
                    ], [
                        "bet_amount" => $this->exchangeRate($stakeAmount, '/'),
                        "win_amount" => $this->exchangeRate($payoffAmount, '/'),
                        "vendor_bet_amount" => $stakeAmount,
                        "vendor_win_amount" => $payoffAmount,
                        "settled_time" => $payoffTime,
                        "total" => $total,
                        "updated_at" => new UTCDateTime(),
                        "deleted_at" => new UTCDateTime(),
                        "status" => TransactionConst::PAYOFF,
                    ]);
                    if ($result !== false) {
                        $this->precountFix($this->opCode, $this->vendorCode, $this->parentBetId, $this->betId, $asyncSettledLog);
                        return true;
                    }
                }
            }
        } catch(\Throwable $th) {
            throw new Exception($th->getMessage());
        }

        return false;
    }

    /**
     * 標記為 派彩 (結算) 略過檢查下注
     * @param float $payoffAmount 結算金額
     * @param int $payoffTime 結算時間
     * @param int $total 注單數量
     *
     * @return bool
     * @throws Exception
     */
    public function payoffSkipCheck(float $payoffAmount, int $payoffTime, int $total) {
        try {
            $payoffTime = $this->toTime13($payoffTime);
            $updateTime = $payoffTime;

            $playerName = $this->member['player_name'];
            $asyncSettledLog = $this->asyncSettledLog($this->opCode, $this->vendorCode, $playerName, $this->parentBetId, $this->betId);
            if (!empty($asyncSettledLog)) {
                if ($updateTime > $asyncSettledLog['settled_time']) {
                    $result = $this->dbManager->opMongoDb($this->opCode)->updateRow(
                        $this->asyncSettledCol,
                        [
                            'vendor_code' => $this->vendorCode,
                            'player_name' => $playerName,
                            'parent_bet_id' => $this->parentBetId,
                            'bet_id' => $this->betId,
                        ],
                        [
                            "win_amount" => $this->exchangeRate($payoffAmount, '/'),
                            "vendor_win_amount" => $payoffAmount,
                            "settled_time" => $payoffTime,
                            "total" => $total,
                            "updated_at" => new UTCDateTime(),
                            "deleted_at" => new UTCDateTime(),
                            "status" => TransactionConst::PAYOFF,
                        ]
                    );
                    if ($result !== false) {
                        $this->precountFix($this->opCode, $this->vendorCode, $this->parentBetId, $this->betId, $asyncSettledLog);
                        return true;
                    }
                }
            } else {
                throw new Exception("opCode:{$this->opCode} pBetId:{$this->parentBetId} betId:{$this->betId} asyncSettledLog not exists");
            }
        } catch (\Throwable $th) {
            throw new Exception($th->getMessage());
        }

        return false;
    }

    /**
     * 標記為 取消下注 (結算)
     * @param int $stakeTime 下注時間
     * @param int $payoffTime 結算時間
     * @param int $total 注單數量
     *
     * @return bool
     * @throws Exception
     */
    public function cancelStake(int $stakeTime, int $payoffTime, int $total)
    {
        try {
            $hasCreateStake = $this->stake(0, $stakeTime);
            $updateTime = $payoffTime;

            $playerName = $this->member["player_name"];
            // $memberCode = $this->member["member_code"];

            // $stakeTime = $this->toTime13($stakeTime);
            $updateTime = $this->toTime13($updateTime);

            $asyncSettledLog = $this->asyncSettledLog($this->opCode, $this->vendorCode, $playerName, $this->parentBetId, $this->betId);
            if (!empty($asyncSettledLog)) {
                if ($hasCreateStake || ($updateTime > $asyncSettledLog["settled_time"])) {
                    $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->asyncSettledCol, [
                        "vendor_code" => $this->vendorCode,
                        "player_name" => $playerName,
                        "parent_bet_id" => $this->parentBetId,
                        "bet_id" => $this->betId,
                    ], [
                        "bet_amount" => 0,
                        "vendor_bet_amount" => 0,
                        "win_amount" => 0,
                        "vendor_win_amount" => 0,
                        "settled_time" => $updateTime,
                        "total" => $total,
                        "updated_at" => new UTCDateTime(),
                        "deleted_at" => new UTCDateTime(),
                        "status" => TransactionConst::CANCEL_STAKE,
                    ]);
                    if ($result !== false) {
                        $this->precountFix($this->opCode, $this->vendorCode, $this->parentBetId, $this->betId, $asyncSettledLog);
                        return true;
                    }
                }
            }
        } catch (\Throwable $th){
            throw new Exception($th->getMessage());
        }

        return false;
    }

    /**
     * 標記為 取消下注 (結算) 略過檢查下注
     * @param int $payoffTime 結算時間
     * @param int $total 注單數量
     *
     * @return bool
     * @throws Exception
     */
    public function cancelStakeSkipCheck(int $payoffTime, int $total) {
        try {
            $updateTime = $payoffTime;
            $playerName = $this->member['player_name'];
            $updateTime = $this->toTime13($updateTime);

            $asyncSettledLog = $this->asyncSettledLog($this->opCode, $this->vendorCode, $playerName, $this->parentBetId, $this->betId);
            if (!empty($asyncSettledLog)) {
                if ($updateTime > $asyncSettledLog['settled_time']) {
                    $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->asyncSettledCol, [
                        "vendor_code" => $this->vendorCode,
                        "player_name" => $playerName,
                        "parent_bet_id" => $this->parentBetId,
                        "bet_id" => $this->betId,
                    ], [
                        "bet_amount" => 0,
                        "vendor_bet_amount" => 0,
                        "win_amount" => 0,
                        "vendor_win_amount" => 0,
                        "settled_time" => $updateTime,
                        "total" => $total,
                        "updated_at" => new UTCDateTime(),
                        "deleted_at" => new UTCDateTime(),
                        "status" => TransactionConst::CANCEL_STAKE,
                    ]);
                    if ($result !== false) {
                        $this->precountFix($this->opCode, $this->vendorCode, $this->parentBetId, $this->betId, $asyncSettledLog);
                        return true;
                    }
                }
            } else {
                throw new Exception("opCode:{$this->opCode} pBetId:{$this->parentBetId} betId:{$this->betId} asyncSettledLog not exists");
            }

        } catch (\Throwable $th) {
            throw new Exception($th->getMessage());
        }
        return false;
    }

    /**
     * 標記為 取消派彩 (未結算)
     * @param float $stakeAmount 下注金額
     * @param int $stakeTime 下注時間
     * @param int $updateTime 更新時間
     * @param int $total 注單數量
     *
     * @return bool
     * @throws Exception
     */
    public function cancelPayoff(float $stakeAmount, int $stakeTime, int $updateTime, int $total)
    {
        try {
            $hasCreateStake = $this->stake(0, $stakeTime);

            $playerName = $this->member["player_name"];
            $memberCode = $this->member["member_code"];

            $stakeTime = $this->toTime13($stakeTime);
            $updateTime = $this->toTime13($updateTime);

            $asyncSettledLog = $this->asyncSettledLog($this->opCode, $this->vendorCode, $playerName, $this->parentBetId, $this->betId);
            if (!empty($asyncSettledLog)) {
                if ($hasCreateStake || ($updateTime > $asyncSettledLog["settled_time"])) {
                    $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->asyncSettledCol, [
                        "vendor_code" => $this->vendorCode,
                        "player_name" => $playerName,
                        "parent_bet_id" => $this->parentBetId,
                        "bet_id" => $this->betId,
                    ], [
                        "total" => $total,
                        "settled_time" => $updateTime,
                        "updated_at" => new UTCDateTime(),
                        "deleted_at" => "",
                        "status" => TransactionConst::CANCEL_PAYOFF,
                    ]);

                    if ($result !== false) {
                        $this->precountFix($this->opCode, $this->vendorCode, $this->parentBetId, $this->betId, $asyncSettledLog);
                        return true;
                    }
                }
            }
        } catch (\Throwable $th) {
            throw new Exception($th->getMessage());
        }

        return false;
    }

    /**
     * 標記為 取消派彩 (未結算) 略過檢查下注
     * @param int $updateTime 更新時間
     * @param int $total 注單數量
     *
     * @return bool
     * @throws Exception
     */
    public function cancelPayoffSkipCheck(int $updateTime, int $total)
    {
        try {
            $playerName = $this->member['player_name'];
            $memberCode = $this->member['member_code'];
            $updateTime = $this->toTime13($updateTime);
            $asyncSettledLog = $this->asyncSettledLog($this->opCode, $this->vendorCode, $playerName, $this->parentBetId, $this->betId);
            if (!empty($asyncSettledLog)) {
                if ($updateTime > $asyncSettledLog['settled_time']) {
                    $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->asyncSettledCol, [
                        'vendor_code' => $this->vendorCode,
                        'player_name' => $playerName,
                        'parent_bet_id' => $this->parentBetId,
                        'bet_id' => $this->betId,
                    ], [
                        'total' => $total,
                        'settled_time' => $updateTime,
                        'updated_at' => new UTCDateTime(),
                        'deleted_at' => '',
                        'status' => TransactionConst::CANCEL_PAYOFF,
                    ]);

                    if ($result !== false) {
                        $this->precountFix($this->opCode, $this->vendorCode, $this->parentBetId, $this->betId, $asyncSettledLog);
                        return true;
                    }
                }
            } else {
                throw new Exception("opCode:{$this->opCode} pBetId:{$this->parentBetId} betId:{$this->betId} asyncSettledLog not exists");
            }

        } catch (\Throwable $th) {
            throw new Exception($th->getMessage());
        }
        return false;
    }

    /**
     * 標記為 重新下注 (未結算)
     * @param float $stakeAmount 下注金額
     * @param int $stakeTime 下注時間
     * @param int $total 注單數量
     *
     * @return bool
     * @throws Exception
     */
    public function reStake(float $stakeAmount, int $stakeTime, int $total)
    {
        try {
            $hasCreateStake = $this->stake(0, $stakeTime);
            $updateTime = micro_timestamp();

            $playerName = $this->member["player_name"];
            $memberCode = $this->member["member_code"];

            $asyncSettledLog = $this->asyncSettledLog($this->opCode, $this->vendorCode, $playerName, $this->parentBetId, $this->betId);
            if (!empty($asyncSettledLog)) {
                if ($hasCreateStake || ($updateTime > $asyncSettledLog["settled_time"])) {
                    $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->asyncSettledCol, [
                        "vendor_code" => $this->vendorCode,
                        "player_name" => $playerName,
                        "parent_bet_id" => $this->parentBetId,
                        "bet_id" => $this->betId,
                    ], [
                        "bet_amount" => $this->exchangeRate($stakeAmount, '/'),
                        "vendor_bet_amount" => $stakeAmount,
                        "total" => $total,
                        "updated_at" => new UTCDateTime(),
                        "deleted_at" => "",
                        "status" => 'restake',
                    ]);
                    if ($result !== false) {
                        $this->precountFix($this->opCode, $this->vendorCode, $this->parentBetId, $this->betId, $asyncSettledLog);
                        return true;
                    }
                }
            }
        } catch (\Throwable $th) {
            throw new \Exception($th->getMessage());
        }

        return false;
    }


    /**
     * 查詢注單
     * @param string $vendorCode
     * @param string $playerName
     * @param string $parentBetId
     * @param string $betId
     * @return mixed|null
     */
    private function asyncSettledLog(string $opCode, string $vendorCode, string $playerName, string $parentBetId, string $betId)
    {
        $log = $this->dbManager->opMongoDb($opCode)->fetchAll($this->asyncSettledCol, [
            "vendor_code" => $vendorCode,
            "player_name" => $playerName,
            "parent_bet_id" => $parentBetId,
            "bet_id" => $betId,
        ]);
        return (!empty($log[0])) ? $log[0] : null;
    }

    /**
     * 觸發修正 precount
     * @param string $opCode
     * @param string $vendorCode
     * @param string $parentBetId
     * @param string $betId
     * @param array $asyncSettledLog
     * @return bool
     */
    private function precountFix(string $opCode, string $vendorCode, string $parentBetId, string $betId, array $asyncSettledLog)
    {
        if (!empty($asyncSettledLog['settled_time']) && $asyncSettledLog['settled_time'] != 0) {
            # 與現在時間差距 1 小的忽略
            $now = Carbon::now($this->carbonTimeZone);

            $lst = Carbon::createFromTimestamp(substr(strval($asyncSettledLog['settled_time']), 0, 10), $this->carbonTimeZone);
            if ($lst->lt($now->copy()->startOfHour())) {
                $pfRecord = [
                    'type' => 'settled',
                    'op_code' => $opCode,
                    'vendor_code' => $vendorCode,
                    'parent_bet_id' => $parentBetId,
                    'bet_id' => $betId,
                    'player_name' => $asyncSettledLog['player_name'],
                    'bet_amount' => $asyncSettledLog['bet_amount'],
                    'vendor_bet_amount' => $asyncSettledLog['vendor_bet_amount'],
                    'win_amount' => $asyncSettledLog['win_amount'],
                    'vendor_win_amount' => $asyncSettledLog['vendor_win_amount'],
                    'game_code' => $asyncSettledLog['game_code'],
                    'time' => $lst->copy()->timezone('UTC')->format('Y-m-d H'),
                    'status' => 0,
                    'created_at' => new UTCDateTime(),
                    'updated_at' => '',
                ];

                $this->mongodb->setPool($this->mongoDefaultPool)->insert($this->precountFixCol, $pfRecord);

                return true;
            }
        }
        return false;
    }

    /**
     * 時間字串轉 13 位 timestamp
     * @param int $ts
     * @return int
     */
    private function toTime13(int $ts): int
    {
        return intval(str_pad(strval($ts), 13, '0'));
    }

    /**
     * 幣值轉換
     * @param float $amount
     * @param string $operator
     * @return false|float
     */
    protected function exchangeRate(float $amount, string $operator) {
        $amount = strval($amount);
        $rate = strval($this->currencyRate);
        switch ($operator) {
            case '*':
                return floatval(bcmul($amount, $rate, $this->currencyScale));
            case '/':
                return floatval(bcdiv($amount, $rate, $this->currencyScale));
            default :
                return  false;
        }
    }
}