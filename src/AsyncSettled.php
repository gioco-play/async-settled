<?php


namespace GiocoPlus\AsyncSettled;


use Carbon\Carbon;
use GiocoPlus\Mongodb\MongoDb;
use GiocoPlus\PrismConst\Constant\TransactionConst;
use GiocoPlus\PrismPlus\Repository\DbManager;
use Hyperf\Utils\ApplicationContext;
use MongoDB\BSON\UTCDateTime;

class AsyncSettled
{
    /**
     * @var DbManager
     */
    private $dbManager;

    /**
     * @var string 營商代碼
     */
//    protected $opCode;

    /**
     * @var string 遊戲商代碼
     */
//    protected $vendorCode;

    /**
     * @var string 遊戲代碼
     */
//    protected $gameCode;

    /**
     * @var string 父注編號
     */
//    protected $parentBetId;

    /**
     * @var string 下注編號
     */
//    protected $betId;

    /**
     * @var string 玩家名稱
     */
//    protected $playerName;

    /**
     * @var string 玩家代碼
     */
//    protected $memberCode;

    /**
     * @var array
     */
//    protected $lastLog;

    /**
     * @var string
     */
    private $asyncSettledCol;

    /**
     * @var string
     */
    private $prcountFixCol;

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

//    public function __construct(ContainerInterface $container)
    public function __construct()
    {
//        $this->dbManager = $container->get(DbManager::class);
//        $this->mongodb = $container->get(MongoDb::class);

        $this->dbManager = ApplicationContext::getContainer()->get(DbManager::class);
        $this->mongodb = ApplicationContext::getContainer()->get(MongoDb::class);

        $this->asyncSettledCol = "async_settled";
        $this->prcountFixCol = "precount_fix";
    }

    /**
     *
     * @param string $opCode 營商代碼
     * @param string $vendorCode 遊戲商代碼
     * @param string $gameCode 遊戲代碼
     * @param string $parentBetId 父注編號
     * @param string $betId 下注編號
     * @param array $member memberInfo
     */
//    public function setDefault(string $opCode, string $vendorCode, string $gameCode, string $parentBetId, string $betId, array $member)
//    {
//        $this->opCode = $opCode;
//        $this->vendorCode = $vendorCode;
//        $this->gameCode = $gameCode;
//        $this->parentBetId = $parentBetId;
//        $this->betId = $betId;
//
//        $this->playerName = $member["player_name"];
//        $this->memberCode = $member["member_code"];
//
//        return $this;
//    }

    /**
     * 標記為 下注 (未結算)
     * @param string $opCode 營商代碼
     * @param string $vendorCode 遊戲商代碼
     * @param string $gameCode 遊戲代碼
     * @param string $parentBetId 父注編號
     * @param string $betId 下注編號
     * @param array $member key 需 player_name & member_code
     *
     * @param float $stakeAmount 下注金額
     * @param int $stakeTime 下注時間
     * @return bool
     */
    public function stake(
        string $opCode, string $vendorCode, string $gameCode, string $parentBetId, string $betId, array $member,
        float $stakeAmount, int $stakeTime)
    {
        $playerName = $member["player_name"];
        $memberCode = $member["member_code"];

        if (empty($this->asyncSettledLog($opCode, $vendorCode, $playerName, $parentBetId, $betId))) {
            $record = [
                "vendor_code" => $vendorCode,
                "game_code" => $gameCode,
                "parent_bet_id" => $parentBetId,
                "bet_id" => $betId,
                "player_name" =>$playerName,
                "member_code" => $memberCode,
                "bet_amount" => $stakeAmount,
                "win_amount" => 0,
                "bet_time" => $stakeTime,
                "settled_time" => 0,
                "status" => TransactionConst::STAKE,
                "created_at" => new UTCDateTime(),
                "updated_at" => new UTCDateTime(),
                "deleted_at" => "", # ttl 刪除用 (結算時會標記上時間，其餘該欄位值設為 null)
            ];

            $result = $this->dbManager->opMongoDb($opCode)->insert($this->asyncSettledCol, $record);
            if ($result !== false) {
                return true;
            }
        }

        return false;
    }

    /**
     * 標記為 派彩 (結算)
     * @param string $opCode
     * @param string $vendorCode
     * @param string $gameCode
     * @param string $parentBetId
     * @param string $betId
     * @param array $member
     * @param float $stakeAmount
     * @param int $stakeTime
     * @param float $payoffAmount
     * @param int $payoffTime
     * @return bool
     * @throws \GiocoPlus\Mongodb\Exception\MongoDBException
     */
    public function payoff(
        string $opCode, string $vendorCode, string $gameCode, string $parentBetId, string $betId, array $member,
        float $stakeAmount, int $stakeTime, float $payoffAmount, int $payoffTime)
    {
        $hasCreateStake = $this->stake(
            $opCode, $vendorCode, $gameCode, $parentBetId, $betId, $member,
            $stakeAmount, $stakeTime
        );
        $updateTime = $payoffTime;

        $playerName = $member["player_name"];
        $memberCode = $member["member_code"];

        $asyncSettledLog = $this->asyncSettledLog($opCode, $vendorCode, $playerName, $parentBetId, $betId);
        if (!empty($asyncSettledLog)) {
            if ($hasCreateStake || ($updateTime > $asyncSettledLog["settled_time"])) {
                $result = $this->dbManager->opMongoDb($opCode)->updateRow($this->asyncSettledCol, [
                    "vendor_code" => $vendorCode,
                    "player_name" => $playerName,
                    "parent_bet_id" => $parentBetId,
                    "bet_id" => $betId,
                ], [
                    "bet_amount" => $stakeAmount,
                    "win_amount" => $payoffAmount,
                    "settled_time" => $payoffTime,
                    "updated_at" => new UTCDateTime(),
                    "status" => TransactionConst::PAYOFF,
                ]);
                if ($result !== false) {
                    if (!empty($asyncSettledLog)) {
                        $this->precountFix($opCode, $vendorCode, $parentBetId, $betId, $asyncSettledLog);
                    }
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 標記為 取消下注 (結算)
     * @param string $opCode
     * @param string $vendorCode
     * @param string $gameCode
     * @param string $parentBetId
     * @param string $betId
     * @param array $member
     * @param int $stakeTime
     * @param int $payoffTime
     * @return bool
     * @throws \GiocoPlus\Mongodb\Exception\MongoDBException
     */
    public function cancelStake(
        string $opCode, string $vendorCode, string $gameCode, string $parentBetId, string $betId, array $member,
        int $stakeTime, int $payoffTime)
    {
        $hasCreateStake = $this->stake(
            $opCode, $vendorCode, $gameCode, $parentBetId, $betId, $member,
            0, $stakeTime
        );
        $updateTime = $payoffTime;

        $playerName = $member["player_name"];
        $memberCode = $member["member_code"];

        $asyncSettledLog = $this->asyncSettledLog($opCode, $vendorCode, $playerName, $parentBetId, $betId);
        if (!empty($asyncSettledLog)) {
            if ($hasCreateStake || ($updateTime > $asyncSettledLog["settled_time"])) {
                $result = $this->dbManager->opMongoDb($opCode)->updateRow($this->asyncSettledCol, [
                    "vendor_code" => $vendorCode,
                    "player_name" => $playerName,
                    "parent_bet_id" => $parentBetId,
                    "bet_id" => $betId,
                ], [
                    "bet_amount" => 0,
                    "win_amount" => 0,
                    "settled_time" => $updateTime,
                    "updated_at" => new UTCDateTime(),
                    "status" => TransactionConst::CANCEL_STAKE,
                ]);
                if ($result !== false) {
                    if (!empty($asyncSettledLog)) {
                        $this->precountFix($opCode, $vendorCode, $parentBetId, $betId, $asyncSettledLog);
                    }
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 標記為 取消派彩 (未結算)
     * @param string $opCode
     * @param string $vendorCode
     * @param string $gameCode
     * @param string $parentBetId
     * @param string $betId
     * @param array $member
     * @param float $stakeAmount
     * @param int $stakeTime
     * @param int $updateTime
     * @return bool
     * @throws \GiocoPlus\Mongodb\Exception\MongoDBException
     */
    public function cancelPayoff(
        string $opCode, string $vendorCode, string $gameCode, string $parentBetId, string $betId, array $member,
        float $stakeAmount, int $stakeTime, int $updateTime)
    {
        $hasCreateStake = $this->stake(
            $opCode, $vendorCode, $gameCode, $parentBetId, $betId, $member,
            0, $stakeTime
        );

        $playerName = $member["player_name"];
        $memberCode = $member["member_code"];

        $asyncSettledLog = $this->asyncSettledLog($opCode, $vendorCode, $playerName, $parentBetId, $betId);
        if (!empty($asyncSettledLog)) {
            if ($hasCreateStake || ($updateTime > $asyncSettledLog["settled_time"])) {
                $result = $this->dbManager->opMongoDb($opCode)->updateRow($this->asyncSettledCol, [
                    "vendor_code" => $vendorCode,
                    "player_name" => $playerName,
                    "parent_bet_id" => $parentBetId,
                    "bet_id" => $betId,
                ], [
                    "settled_time" => $updateTime,
                    "updated_at" => new UTCDateTime(),
                    "status" => TransactionConst::CANCEL_PAYOFF,
                ]);

                if ($result !== false) {
                    if (!empty($asyncSettledLog)) {
                        $this->precountFix($opCode, $vendorCode, $parentBetId, $betId, $asyncSettledLog);
                    }
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * 標記為 重新下注 (未結算)
     * @param float $stakeAmount
     * @param int $stakeTime
     * @return bool
     * @throws \GiocoPlus\Mongodb\Exception\MongoDBException
     */
    public function reStake(
        string $opCode, string $vendorCode, string $gameCode, string $parentBetId, string $betId, array $member,
        float $stakeAmount, int $stakeTime)
    {
        $hasCreateStake = $this->stake(
            $opCode, $vendorCode, $gameCode, $parentBetId, $betId, $member,
            0, $stakeTime
        );
        $updateTime = micro_timestamp();

        $playerName = $member["player_name"];
        $memberCode = $member["member_code"];

        $asyncSettledLog = $this->asyncSettledLog($opCode, $vendorCode, $playerName, $parentBetId, $betId);
        if (!empty($asyncSettledLog)) {
            if ($hasCreateStake || ($updateTime > $asyncSettledLog["settled_time"])) {
                $result = $this->dbManager->opMongoDb($opCode)->updateRow($this->asyncSettledCol, [
                    "vendor_code" => $vendorCode,
                    "player_name" => $playerName,
                    "parent_bet_id" => $parentBetId,
                    "bet_id" => $betId,
                ], [
                    "bet_amount" => $stakeAmount,
                    "updated_at" => new UTCDateTime(),
                    "status" => 'restake',
                ]);
                if ($result !== false) {
                    if (!empty($asyncSettledLog)) {
                        $this->precountFix($opCode, $vendorCode, $parentBetId, $betId, $asyncSettledLog);
                    }
                    return true;
                }
            }
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
     * @throws \GiocoPlus\Mongodb\Exception\MongoDBException
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
     * @param array $asyncSettledLog
     * @return bool
     * @throws \GiocoPlus\Mongodb\Exception\MongoDBException
     */
    private function precountFix(string $opCode, string $vendorCode, string $parentBetId, string $betId, array $asyncSettledLog)
    {
        if (!empty($asyncSettledLog["settled_time"])) {
            # 與現在時間差距 1 小的忽略
            $now = Carbon::now($this->carbonTimeZone);
            $lst = Carbon::createFromTimestamp(substr($asyncSettledLog["settled_time"], 0, 10), $this->carbonTimeZone);
            if ($lst->lt($now->copy()->startOfHour()->subHour(1))) {
                $pfRecord = [
                    "type" => "settled",
                    "op_code" => $opCode,
                    "vendor_code" => $vendorCode,
                    "parent_bet_id" => $parentBetId,
                    "bet_id" => $betId,
                    "player_name" => $asyncSettledLog["player_name"],
                    "bet_amount" => $asyncSettledLog["bet_amount"],
                    "win_amount" => $asyncSettledLog["win_amount"],
                    "game_code" => $asyncSettledLog["game_code"],
                    "time" => $lst->format("Y-m-d H"),
                    "created_at" => new UTCDateTime()
                ];
                $this->mongodb->setPool($this->mongoDefaultPool)->insert($this->prcountFixCol, $pfRecord);

                return true;
            }
        }
        return false;
    }

}