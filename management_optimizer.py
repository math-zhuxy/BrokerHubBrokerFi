import numpy as np
from typing import List, Dict, Tuple
from src.utils.logger import Logger
import numpy as np
from typing import List, Dict, Tuple
from src.utils.logger import Logger

class ManagementFeeOptimizer:
    def __init__(self, config: Dict):
        # 基本参数
        self.id = config["params"]["id"]
        self.initial_funds = config["params"]["initial_funds"]
        self.min_fee_rate = 0.05  # 5% 最低费率
        self.max_fee_rate = 0.99  # 15% 最高费率
        self.base_fee_rate = 0  # 5% 基础费率
        self.logger = Logger.get_logger()

        # 当前状态和历史记录
        self.current_fee_rate = config["params"]["initial_tax_rate"]
        self.fee_rate_history = [self.current_fee_rate]
        self.revenue_history = []
        self.net_revenue_history = []
        self.funds_history = []
        self.invested_funds_history = []  # 新增：记录投资者资金历史
        
        self.window_size = 5  # 用于计算移动平均的窗口大小
        self.adjust_threshold = 0.1  # 收益变化阈值
        
        self.historical_max_successful_rate = self.max_fee_rate  # 新增:记录历史安全费率上限
        self.success_threshold = 5  # 新增:连续成功次数阈值
        self.failure_threshold = 2  # 新增:连续失败次数阈值
        self.consecutive_success = 0  # 新增:连续成功计数
        self.consecutive_failure = 0  # 新增:连续失败计数
        self.status = 0
        self.previous_invested_funds = None  # 新增:上一轮投资者资金
        

    def calculate_adjustment_coefficient(self, total_funds: float, competitor_funds: float, initial_funds: float) -> float:
        """
        根据与竞争对手的有效资金差值倍数计算调整系数
        
        Args:
            total_funds: 当前BrokerHub的总资金量
            competitor_funds: 竞争对手BrokerHub的总资金量
            initial_funds: BrokerHub的初始资金
            
        Returns:
            float: 调整系数 (0.05-0.2之间)
        """
        # 计算实际投资者投入的资金
        invested_funds = total_funds - initial_funds
        # 假设竞争对手也有相同的initial_funds，计算其投资者资金
        competitor_invested_funds = competitor_funds - initial_funds
        
        if invested_funds <= 0:
            return 0.2  # 如果没有投资者资金，使用最大调整系数
            
        # 计算有效资金差值倍数
        funds_ratio = competitor_invested_funds / invested_funds if invested_funds > 0 else float('inf')
        
        # 根据倍数差异设置调整系数
        if funds_ratio > 2:  # 对手投资者资金超过2倍
            return 0.2
        elif funds_ratio > 1.5:  # 对手投资者资金超过1.5倍
            return 0.15
        elif funds_ratio > 1.2:  # 对手投资者资金超过1.2倍
            return 0.1
        else:  # 资金差距相对较小
            return 0.05
            
    def update_rate_bounds(self, net_revenue: float):
        """
        根据投资者资金变化更新费率上限
        """
        if len(self.net_revenue_history) <= 5 or len(self.invested_funds_history) <= 5 or len(self.fee_rate_history) <= 5:
            return
            
       
        # 计算收益趋势
        recent_revenues = self.net_revenue_history[-5:]
        x = np.arange(5)
        revenue_slope = np.polyfit(x, recent_revenues, 1)[0]
        revenue_change = revenue_slope / max(abs(np.mean(recent_revenues)), 1)
        
        # 计算资金趋势
        recent_funds = self.invested_funds_history[-5:]
        funds_slope = np.polyfit(x, recent_funds, 1)[0]
        funds_change = funds_slope / max(abs(np.mean(recent_funds)), 1)
        
        # 计算费率趋势
        recent_rates = self.fee_rate_history[-5:]
        rate_slope = np.polyfit(x, recent_rates, 1)[0]
        rate_change = rate_slope / max(abs(np.mean(recent_rates)), 1)
    
    

        # print(self.id, " ", funds_change)
        # self.logger.info(f"BrokerHub {self.id} - funds_change: {funds_change}")
            
        # 记录诊断信息
        self.logger.info(f"BrokerHub {self.id} - Diagnostics: revenue_change={revenue_change:.6f}, " +
                        f"funds_change={funds_change:.6f}, rate_change={rate_change:.6f}，consecutive_success={self.consecutive_success}")
    
        if(self.status == 0 and revenue_change != 0.0 and abs(funds_change) <= 1e-10):
            self.consecutive_failure = 0
            self.consecutive_success = 0
            if(revenue_change < 0):
                self.historical_max_successful_rate = max(
                    self.historical_max_successful_rate,
                    self.current_fee_rate * 1.0001
                )
            if(revenue_change > 0):
                self.historical_max_successful_rate = max(
                    self.historical_max_successful_rate,
                    self.current_fee_rate * 0.9999
                )
            self.status = 1
            self.logger.info(f"BrokerHub {self.id} - Market saturation persists considering stable rates: {self.historical_max_successful_rate:.4f}")

            return 1
        if(self.status == 1 and funds_change <= 0):
            if(revenue_change > 0):
                self.status = 2
                self.optimal_rate_lower = self.current_fee_rate
                self.optimal_rate_upper = self.current_fee_rate
                self.logger.info(f"BrokerHub {self.id} - Status 2: Revenue increasing despite fund decrease, optimal rate may be {self.current_fee_rate:.4f}")

            if(revenue_change <= 0):
                self.status = 3
                self.optimal_rate_lower = min(self.current_fee_rate, self.fee_rate_history[-2])
                self.optimal_rate_upper = max(self.current_fee_rate, self.fee_rate_history[-2])
                self.logger.info(f"BrokerHub {self.id} - Status 3: Optimal rate between {self.optimal_rate_lower:.4f} and {self.optimal_rate_upper:.4f}")
            return 1
        elif(self.status == 1 and funds_change > 0):
            self.logger.info(f"BrokerHub {self.id} - Status reset: fund increase, not optimal fee.")
            self.status = 0
            
            
        # 更新连续成功/失败计数
        if revenue_change >= 1e-5 or funds_change >= 1e-5:  
            self.consecutive_success += 1
            self.consecutive_failure = 0
            
            # 如果连续成功足够多次,认为当前费率是安全的
            if self.consecutive_success >= self.success_threshold:
                self.historical_max_successful_rate = max(
                    self.historical_max_successful_rate,
                    self.current_fee_rate
                )
                self.logger.info(f"BrokerHub {self.id} - Ideal growth, updating max successful rate to: {self.historical_max_successful_rate:.4f}")
    
                # self.consecutive_success = 0
          
        # elif revenue_change >= 1e-5 and funds_change < 0:
            # 收益增长但资金减少：可能是费率调整有效但过高
            # 保持现状，不增加成功或失败计数
            # self.logger.info(f"BrokerHub {self.id} - Revenue growing but losing funds, maintaining current strategy")
          
        elif revenue_change <= -1e-5:  # 1%的负增长视为失败
                
                
            self.consecutive_failure += 1
            self.consecutive_success = 0
            
            # 如果连续失败,可能需要调整历史最大安全费率
            if self.consecutive_failure >= self.failure_threshold:
                self.historical_max_successful_rate = min(
                    self.historical_max_successful_rate,
                    self.current_fee_rate * 0.95  # 稍微降低历史最大安全费率
                )
                # self.consecutive_failure = 0
                
                self.logger.info(f"BrokerHub {self.id} - decrease, updating max successful rate to: {self.historical_max_successful_rate:.4f}")
        
    def is_brokerhub_id(self, id: int) -> bool:
        """判断是否为BrokerHub ID"""
        return str(id).startswith('BrokerHub')

    def optimize(self, iteration: int, last_b2e_rates: Dict[int, float], 
                last_b2e_earnings: Dict[int, float], participation_rate1: float,
                total_investment: float, current_funds: Dict[int, float],
                current_earn: float, transaction_data: List[Tuple[int, int, str, str]]) -> float:
        # 更新历史数据
        current_own_funds = current_funds[self.id]
        current_invested_funds = current_own_funds - self.initial_funds
        
        
        previous_invested_funds = self.invested_funds_history[-1] if len(self.invested_funds_history) > 0 else 0
        is_losing_investors = current_invested_funds < previous_invested_funds * 0.9  # 投资者减少10%以上

        # 如果投资者大量流失，立即调整historical_max_successful_rate
        if is_losing_investors and previous_invested_funds > 0:
            # 将历史安全费率调整为当前费率的80%，确保下次不会升得太高
            self.historical_max_successful_rate = min(
                self.historical_max_successful_rate,
                self.fee_rate_history[-2] * 0.8
            )
            self.status = 0
            self.logger.info(f"BrokerHub {self.id} - Investors leaving, adjusting historical_max_successful_rate rate to: {self.historical_max_successful_rate:.4f}")
    
        self.revenue_history.append(current_earn)
        self.funds_history.append(current_own_funds)
        self.invested_funds_history.append(current_invested_funds)
        
        # 计算净收益
        net_revenue = current_earn * (current_invested_funds) / current_own_funds * self.current_fee_rate
        self.net_revenue_history.append(net_revenue)
        # 更新费率范围
        self.update_rate_bounds(net_revenue)
        
    # 添加：如果没有投资者，主动降低费率
        if current_invested_funds <= 0:
            self.consecutive_success = 0
            self.consecutive_failure = 0
        
            if(previous_invested_funds != 0 and len(self.fee_rate_history) >= 2):
                self.current_fee_rate = self.fee_rate_history[-2] * 1.05
                self.fee_rate_history.append(self.current_fee_rate)
                self.logger.info(f"BrokerHub {self.id} - No investors, and last epoch has investor, return to last fee rate: {self.current_fee_rate:.4f}")
                return self.current_fee_rate
            # 根据迭代次数逐步降低费率
            # no_investor_adjustment = -0.01 * min(iteration, 10) / 10  # 最多降低1个百分点
            no_investor_adjustment = - self.current_fee_rate / 2  # 最多降低1个百分点
            # 计算新费率，但确保不低于最低费率
            new_fee_rate = max(self.min_fee_rate, self.current_fee_rate + no_investor_adjustment)
            
            self.current_fee_rate = new_fee_rate
            self.fee_rate_history.append(self.current_fee_rate)
            
            self.logger.info(f"BrokerHub {self.id} - No investors, reducing fee rate: {self.current_fee_rate:.4f}")
            
            return self.current_fee_rate
        # 在前两轮先收集数据
        if iteration <= 2:
            return self.current_fee_rate   
        
        # 获取竞争对手数据
        competitor_funds = {k: v for k, v in current_funds.items() 
                          if str(k).startswith('BrokerHub') and k != self.id}     
        if not competitor_funds:
            return self.current_fee_rate
          
        # 找出资金最多的竞争对手
        max_competitor_id = max(competitor_funds.items(), key=lambda x: x[1])[0]
        max_competitor_funds = competitor_funds[max_competitor_id]
        competitor_invested_funds = max_competitor_funds - self.initial_funds
        
        # 计算与竞争对手的资金差距比例
        funds_diff_ratio = (competitor_invested_funds - current_invested_funds) / max(competitor_invested_funds, 1e18)
        
        # 获取调整系数
        adjustment_coef = self.calculate_adjustment_coefficient(
            current_own_funds,
            max_competitor_funds,
            self.initial_funds
        )
        
        # 计算基于竞争的费率调整
        if funds_diff_ratio > 0:  # 落后情况
            # 检查竞争对手收益率
            competitor_rate = last_b2e_rates[max_competitor_id]
            own_rate = last_b2e_rates[self.id]
            
            if own_rate < competitor_rate:
                # 收益率也落后，需要更激进地降低费率
                competition_adjustment = -adjustment_coef * funds_diff_ratio
            else:
                # 收益率领先，温和调整
                competition_adjustment = -adjustment_coef * funds_diff_ratio * 0.5
        else:  # 领先情况
            # 可以适当提高费率
            competition_adjustment = adjustment_coef * abs(funds_diff_ratio) * 0.3
        # 设置参数
        rapid_decay_threshold = 100  # 快速衰减阈值
        initial_bound = 0.9          # 初始bounds值
        min_bound = 0.1              # 线性衰减的最小值
        
        # bounds计算函数
        def calculate_bounds(iteration):
            if iteration > rapid_decay_threshold:
                # 超过阈值后快速衰减
                return (0.1 * rapid_decay_threshold)/iteration
            else:
                # 从0开始线性衰减(从0.9降至0.1)
                decay_rate = (initial_bound - min_bound) / rapid_decay_threshold
                return initial_bound - iteration * decay_rate
                
        bounds = calculate_bounds(iteration)
        lower_bound = self.min_fee_rate  # 下限不低于min_fee_rate
        
        upper_bound = min(self.max_fee_rate, self.historical_max_successful_rate * ( 1 + bounds))  # 上限不超过max_fee_rate
        if(self.status >= 2):
            lower_bound = self.optimal_rate_lower
            upper_bound = self.optimal_rate_upper    

        # 在范围内应用adjustment
        new_fee_rate = max(lower_bound, min(upper_bound, self.current_fee_rate + competition_adjustment))
        
        # 更新当前费率
        self.current_fee_rate = new_fee_rate
        self.fee_rate_history.append(self.current_fee_rate)
        
        self.logger.info(f"BrokerHub {self.id} - New fee rate: {self.current_fee_rate:.4f}, "
                        f"Funds diff ratio: {funds_diff_ratio:.4f}, "
                        f"Current invested funds: {current_invested_funds}, "
                        f"Current min_fee_rate: {self.min_fee_rate}, "
                        f"Current max_fee_rate: {self.max_fee_rate}, "
                        f"Current historical_max_successful_rate: {self.historical_max_successful_rate}, "
                        f"Competitor invested funds: {competitor_invested_funds}")
        return self.current_fee_rate
        