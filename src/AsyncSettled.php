<?php


namespace GiocoPlus\AsyncSettled;


use Carbon\Carbon;
use GiocoPlus\Mongodb\MongoDb;
use GiocoPlus\PrismConst\Constant\TransactionConst;
use GiocoPlus\PrismPlus\Repository\DbManager;
use Hyperf\Di\Annotation\Inject;
use MongoDB\BSON\UTCDateTime;
use Psr\Container\ContainerInterface;

class AsyncSettled
{
    /**
     * @var DbManager
     */
    private $dbManager;

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
     * @var string 玩家名稱
     */
    protected $playerName;

    /**
     * @var string 玩家代碼
     */
    protected $memberCode;

    /**
     * @var array
     */
    protected $lastLog;

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

    public function __construct(ContainerInterface $container)
    {
        $this->dbManager = $container->get(DbManager::class);
        $this->mongodb = $container->get(MongoDb::class);
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
    public function setDefault(string $opCode, string $vendorCode, string $gameCode, string $parentBetId, string $betId, array $member)
    {
        $this->opCode = $opCode;
        $this->vendorCode = $vendorCode;
        $this->gameCode = $gameCode;
        $this->parentBetId = $parentBetId;
        $this->betId = $betId;

        $this->playerName = $member["player_name"];
        $this->memberCode = $member["member_code"];

        return $this;
    }

    /**
     * @param float $stakeAmount 下注金額
     * @param int $stakeTime 下注時間
     * @return bool
     */
    public function stake(float $stakeAmount, int $stakeTime)
    {
        $this->lastLog = $this->_lastLog();

        if (empty($this->lastLog)) {
            $updateTime = micro_timestamp();

            $record = [
                "vendor_code" => $this->vendorCode,
                "game_code" => $this->gameCode,
                "parent_bet_id" => $this->parentBetId,
                "bet_id" => $this->betId,
                "player_name" =>$this->playerName,
                "member_code" => $this->memberCode,
                "bet_amount" => $stakeAmount,
                "win_amount" => 0,
                "bet_time" => $stakeTime,
                "settled_time" => 0,
                "status" => TransactionConst::STAKE,
                "created_at" => new UTCDateTime(),
                "updated_at" => new UTCDateTime(),
                "deleted_at" => "", # 刪除用
            ];

            $result = $this->dbManager->opMongoDb($this->opCode)->insert($this->asyncSettledCol, $record);
            if ($result !== false) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param float $stakeAmount
     * @param int $stakeTime
     * @param float $payoffAmount
     * @param int $payoffTime
     * @return bool
     * @throws \GiocoPlus\Mongodb\Exception\MongoDBException
     */
    public function payoff(float $stakeAmount, int $stakeTime, float $payoffAmount, int $payoffTime)
    {
        $hasCreateStake = $this->stake($stakeAmount, $stakeTime);
        $updateTime = $payoffTime;

        $lastLog = $this->_lastLog();
        if (!empty($lastLog)) {
            if ($hasCreateStake || ($updateTime > $lastLog["settled_time"])) {
                $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->asyncSettledCol, [
                    "vendor_code" => $this->vendorCode,
                    "player_name" => $this->playerName,
                    "parent_bet_id" => $this->parentBetId,
                    "bet_id" => $this->betId,
                ], [
                    "bet_amount" => $stakeAmount,
                    "win_amount" => $payoffAmount,
                    "settled_time" => $payoffTime,
                    "updated_at" => new UTCDateTime(),
                    "status" => TransactionConst::PAYOFF,
                ]);
                if ($result !== false) {
                    if (!empty($lastLog)) {
                        $this->precountFix($lastLog);
                    }
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * @param int $stakeTime
     * @param int $payoffTime
     * @return bool
     * @throws \GiocoPlus\Mongodb\Exception\MongoDBException
     */
    public function cancelStake(int $stakeTime, int $payoffTime)
    {
        $hasCreateStake = $this->stake(0, $stakeTime);
        $updateTime = $payoffTime;

        $lastLog = $this->_lastLog();
        if (!empty($lastLog)) {
            if ($hasCreateStake || ($updateTime > $lastLog["settled_time"])) {
                $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->asyncSettledCol, [
                    "vendor_code" => $this->vendorCode,
                    "player_name" => $this->playerName,
                    "parent_bet_id" => $this->parentBetId,
                    "bet_id" => $this->betId,
                ], [
                    "bet_amount" => 0,
                    "win_amount" => 0,
                    "settled_time" => $updateTime,
                    "updated_at" => new UTCDateTime(),
                    "status" => TransactionConst::CANCEL_STAKE,
                ]);
                if ($result !== false) {
                    if (!empty($lastLog)) {
                        $this->precountFix($lastLog);
                    }
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * @param float $stakeAmount
     * @param int $stakeTime
     * @param int $updateTime
     * @return bool
     * @throws \GiocoPlus\Mongodb\Exception\MongoDBException
     */
    public function cancelPayoff(float $stakeAmount, int $stakeTime, int $updateTime)
    {
        $hasCreateStake = $this->stake($stakeAmount, $stakeTime);

        $lastLog = $this->_lastLog();
        if (!empty($lastLog)) {
            if ($hasCreateStake || ($updateTime > $lastLog["settled_time"])) {
                $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->asyncSettledCol, [
                    "vendor_code" => $this->vendorCode,
                    "player_name" => $this->playerName,
                    "parent_bet_id" => $this->parentBetId,
                    "bet_id" => $this->betId,
                ], [
                    "settled_time" => $updateTime,
                    "updated_at" => new UTCDateTime(),
                    "status" => TransactionConst::CANCEL_PAYOFF,
                ]);

                if ($result !== false) {
                    if (!empty($lastLog)) {
                        $this->precountFix($lastLog);
                    }
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * @param float $stakeAmount
     * @param int $stakeTime
     * @return bool
     * @throws \GiocoPlus\Mongodb\Exception\MongoDBException
     */
    public function reStake(float $stakeAmount, int $stakeTime)
    {
        $hasCreateStake = $this->stake($stakeAmount, $stakeTime);
        $updateTime = micro_timestamp();

        $lastLog = $this->_lastLog();
        if (!empty($lastLog)) {
            if ($hasCreateStake || ($updateTime > $lastLog["settled_time"])) {
                $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->asyncSettledCol, [
                    "vendor_code" => $this->vendorCode,
                    "player_name" => $this->playerName,
                    "parent_bet_id" => $this->parentBetId,
                    "bet_id" => $this->betId,
                ], [
                    "bet_amount" => $stakeAmount,
                    "updated_at" => new UTCDateTime(),
                    "status" => 'restake',
                ]);
                if ($result !== false) {
                    if (!empty($lastLog)) {
                        $this->precountFix($lastLog);
                    }
                    return true;
                }
            }
        }
        return false;
    }


    private function _lastLog()
    {
        $log = $this->dbManager->opMongoDb($this->opCode)->fetchAll($this->asyncSettledCol, [
            "vendor_code" => $this->vendorCode,
            "player_name" => $this->playerName,
            "parent_bet_id" => $this->parentBetId,
            "bet_id" => $this->betId,
        ]);
        return (!empty($log[0])) ? $log[0] : null;
    }

    /**
     * 觸發修正 precount
     * @param array $lastLog
     * @return bool
     * @throws \GiocoPlus\Mongodb\Exception\MongoDBException
     */
    private function precountFix(array $lastLog)
    {
        if (!empty($lastLog["settled_time"])) {
            # 與現在時間差距 1 小的忽略
            $now = Carbon::now($this->carbonTimeZone);
            $lst = Carbon::createFromTimestamp($lastLog["settled_time"], $this->carbonTimeZone);
            if ($lst->lt($now->copy()->startOfHour()->subHour(1))) {
                $pfRecord = [
                    "type" => "settled",
                    "op_code" => $this->opCode,
                    "vendor_code" => $this->vendorCode,
                    "parent_bet_id" => $this->parentBetId,
                    "bet_id" => $this->betId,
                    "player_name" => $lastLog["player_name"],
                    "bet_amount" => $lastLog["bet_amount"],
                    "win_amount" => $lastLog["win_amount"],
                    "game_code" => $lastLog["game_code"],
                    "time" => date("Y-m-d H", substr($lastLog["settled_time"], 0, 10)),
                    "created_at" => new UTCDateTime()
                ];
                $this->mongodb->setPool($this->mongoDefaultPool)->insert($this->prcountFixCol, $pfRecord);

                return true;
            }
        }
        return false;
    }

}