<?php


namespace GiocoPlus\AsyncSettled;


use GiocoPlus\PrismConst\Constant\TransactionConst;
use GiocoPlus\PrismPlus\Repository\DbManager;
use GiocoPlus\PrismPlus\Service\CacheService;
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
    private $col;

    public function __construct(ContainerInterface $container)
    {
        $this->dbManager = $container->get(DbManager::class);
        $this->col = "async_settled";
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

        if (!empty($this->lastLog)) {
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
                "updated_at" => new UTCDateTime(),
                "deleted_at" => "", # 刪除用
                "created_at" => new UTCDateTime(),
            ];

            $result = $this->dbManager->opMongoDb($this->opCode)->insert($this->col, $record);
            if ($result !== false) {
                return true;
            }
        }
        return false;
    }

    /**
     * @param float $stakeAmount
     * @param float $payoffAmount
     * @param int $payoffTime
     */
    public function payoff(float $stakeAmount, float $payoffAmount, int $payoffTime)
    {
        $hasCreateStake = $this->stake($stakeAmount, $payoffTime);
        $updateTime = $payoffTime;

        $lastLog = $this->_lastLog();
        if (!empty($lastLog)) {
            if ($hasCreateStake || ($updateTime > $lastLog["update_time"])) {
                $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->col, [
                    "vendor_code" => $this->vendorCode,
                    "player_name" => $this->playerName,
                    "parent_bet_id" => $this->parentBetId,
                    "bet_id" => $this->betId,
                ], [
                    "stake_amount" => $stakeAmount,
                    "payoff_amount" => $payoffAmount,
                    "payoff_time" => $payoffTime,
                    "update_time" => $updateTime,
                    "updated_at" => new UTCDateTime(),
                    "status" => TransactionConst::PAYOFF,
                ]);
                if ($result !== false) {
                    return true;
                }
            }
        }

        return false;
    }

    public function cancelStake(int $stakeTime, int $payoffTime)
    {
        $hasCreateStake = $this->stake(0, $stakeTime);
        $updateTime = $payoffTime;

        $lastLog = $this->_lastLog();
        if (!empty($lastLog)) {
            if ($hasCreateStake || ($updateTime > $lastLog["update_time"])) {
                $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->col, [
                    "vendor_code" => $this->vendorCode,
                    "player_name" => $this->playerName,
                    "parent_bet_id" => $this->parentBetId,
                    "bet_id" => $this->betId,
                ], [
                    "stake_amount" => 0,
                    "payoff_amount" => 0,
                    "payoff_time" => $updateTime,
                    "update_time" =>  $updateTime,
                    "updated_at" => new UTCDateTime(),
                    "status" => TransactionConst::CANCEL_STAKE,
                ]);
                if ($result !== false) {
                    return true;
                }
            }
        }
        return false;
    }

    public function cancelPayoff(float $stakeAmount, int $stakeTime, int $updateTime)
    {
        $hasCreateStake = $this->stake($stakeAmount, $stakeTime);

        $lastLog = $this->_lastLog();
        if (!empty($lastLog)) {
            if ($hasCreateStake || ($updateTime > $lastLog["update_time"])) {
                $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->col, [
                    "vendor_code" => $this->vendorCode,
                    "player_name" => $this->playerName,
                    "parent_bet_id" => $this->parentBetId,
                    "bet_id" => $this->betId,
                ], [
                    "payoff_time" => $updateTime,
                    "update_time" =>  $updateTime,
                    "updated_at" => new UTCDateTime(),
                    "status" => TransactionConst::CANCEL_PAYOFF,
                ]);

                if ($result !== false) {
                    return true;
                }
            }
        }

        return false;
    }

    public function reStake(float $stakeAmount, int $stakeTime)
    {
        $hasCreateStake = $this->stake($stakeAmount, $stakeTime);
        $updateTime = micro_timestamp();

        $lastLog = $this->_lastLog();
        if (!empty($lastLog)) {
            if ($hasCreateStake || ($updateTime > $lastLog["update_time"])) {
                $result = $this->dbManager->opMongoDb($this->opCode)->updateRow($this->col, [
                    "vendor_code" => $this->vendorCode,
                    "player_name" => $this->playerName,
                    "parent_bet_id" => $this->parentBetId,
                    "bet_id" => $this->betId,
                ], [
                    "stake_amount" => $stakeAmount,
                    "update_time" =>  $updateTime,
                    "updated_at" => new UTCDateTime(),
                    "status" => 'restake',
                ]);
                if ($result !== false) {
                    return true;
                }
            }
        }
        return false;
    }

    private function _lastLog()
    {
        $log = $this->dbManager->opMongoDb($this->opCode)->fetchAll("settlement", [
            "vendor_code" => $this->vendorCode,
            "player_name" => $this->playerName,
            "parent_bet_id" => $this->parentBetId,
            "bet_id" => $this->betId,
        ]);
        return (!empty($log[0])) ? $log[0] : null;
    }



}