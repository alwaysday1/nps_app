"""NPS数据分析核心模块"""
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
from io import BytesIO
import zipfile


class NPSAnalyzer:
    """NPS数据分析器"""
    
    REQUIRED_COLUMNS = [
        '订单号', '成团供应商id', '成团供应商名称', '成团子账号uid', '成团子账号名称',
        '成团出行日期', '跟进人id', '跟进人姓名', '分母V5', '拟合诋毁V5', 
        '拟合推荐V5', '有用户反馈'
    ]
    
    def __init__(self, nps_target: int = 60):
        self.nps_target = nps_target
        self.df: Optional[pd.DataFrame] = None
        self.overall_nps: float = 0
        
    def load_excel(self, file_path_or_buffer) -> Tuple[bool, str]:
        """加载Excel文件"""
        try:
            self.df = pd.read_excel(file_path_or_buffer)
            missing = [col for col in self.REQUIRED_COLUMNS if col not in self.df.columns]
            if missing:
                return False, f"缺少必要列: {', '.join(missing)}"
            self._calculate_overall_nps()
            return True, f"成功加载 {len(self.df)} 条记录，{self.df['成团供应商名称'].nunique()} 个供应商"
        except Exception as e:
            return False, f"文件读取错误: {str(e)}"
    
    def _calculate_overall_nps(self):
        """计算整体NPS"""
        total_denominator = self.df['分母V5'].sum()
        if total_denominator > 0:
            total_detractor = self.df['拟合诋毁V5'].sum()
            total_promoter = (self.df['拟合推荐V5'] * self.df['分母V5']).sum()
            self.overall_nps = ((total_promoter / total_denominator) - (total_detractor / total_denominator)) * 100
    
    def _calc_nps_metrics(self, group: pd.DataFrame) -> Dict:
        """计算NPS相关指标"""
        denominator = group['分母V5'].sum()
        detractor = group['拟合诋毁V5'].sum()
        promoter = (group['拟合推荐V5'] * group['分母V5']).sum()
        
        if denominator > 0:
            detractor_rate = detractor / denominator
            promoter_rate = promoter / denominator
            nps = (promoter_rate - detractor_rate) * 100
        else:
            detractor_rate = promoter_rate = nps = 0
            
        return {
            '订单数': len(group),
            '有效分母': round(denominator, 2),
            '诋毁数': int(detractor),
            '推荐分': round(promoter, 2),
            '诋毁率': round(detractor_rate * 100, 2),
            '推荐率': round(promoter_rate * 100, 2),
            'NPS': round(nps, 2)
        }
    
    def get_overall_analysis(self) -> List[Dict]:
        """获取总体NPS分析（所有供应商）"""
        if self.df is None:
            return []
        
        results = []
        grouped = self.df.groupby(['成团供应商id', '成团供应商名称'])
        
        for (supplier_id, supplier_name), group in grouped:
            metrics = self._calc_nps_metrics(group)
            
            # 计算对整体NPS的贡献
            supplier_weight = metrics['有效分母'] / self.df['分母V5'].sum() if self.df['分母V5'].sum() > 0 else 0
            contribution = (metrics['NPS'] - self.overall_nps) * supplier_weight
            
            results.append({
                '供应商ID': int(supplier_id),
                '供应商名称': supplier_name,
                **metrics,
                '未达目标': '是' if metrics['NPS'] < self.nps_target else '否',
                '对整体贡献': round(contribution, 4),
                '负贡献': '是' if contribution < 0 else '否'
            })
        
        # 按NPS降序排名
        results.sort(key=lambda x: x['NPS'], reverse=True)
        for i, r in enumerate(results, 1):
            r['排名'] = i
            
        return results
    
    def get_supplier_list(self) -> List[Dict]:
        """获取供应商列表"""
        if self.df is None:
            return []
        suppliers = self.df.groupby(['成团供应商id', '成团供应商名称']).size().reset_index(name='订单数')
        return [
            {'id': int(row['成团供应商id']), 'name': row['成团供应商名称'], 'count': int(row['订单数'])}
            for _, row in suppliers.iterrows()
        ]
    
    def get_followup_management(self, supplier_id: int) -> List[Dict]:
        """获取单个供应商的追评管理表"""
        if self.df is None:
            return []
        
        supplier_df = self.df[self.df['成团供应商id'] == supplier_id].copy()
        if supplier_df.empty:
            return []
        
        results = []
        for _, row in supplier_df.iterrows():
            # 确定优先级
            if row['分母V5'] == 1 and row['拟合诋毁V5'] == 1:
                priority = 1
                priority_desc = '分母1+诋毁'
            elif row['分母V5'] == 0.6:
                priority = 2
                priority_desc = '分母0.6'
            else:
                priority = 3
                priority_desc = f"分母{row['分母V5']}"
            
            # 确定追评类型
            if row['有用户反馈'] == 0:
                followup_type = '追好评'
            elif row['有用户反馈'] == 1 and row['拟合诋毁V5'] == 1:
                followup_type = '诋毁回正'
            else:
                followup_type = '无需追评'
            
            results.append({
                '订单号': str(row['订单号']),
                '优先级': priority,
                '优先级说明': priority_desc,
                '追评类型': followup_type,
                '分母V5': row['分母V5'],
                '是否诋毁': '是' if row['拟合诋毁V5'] == 1 else '否',
                '有用户反馈': '是' if row['有用户反馈'] == 1 else '否',
                '推荐得分': row['拟合推荐V5']
            })
        
        # 排序：优先级升序，同优先级按分母降序
        results.sort(key=lambda x: (x['优先级'], -x['分母V5']))
        return results
    
    def get_date_dimension(self, supplier_id: int) -> List[Dict]:
        """获取日期维度分析"""
        if self.df is None:
            return []
        
        supplier_df = self.df[self.df['成团供应商id'] == supplier_id].copy()
        if supplier_df.empty:
            return []
        
        # 按日期分组
        supplier_df['日期'] = pd.to_datetime(supplier_df['成团出行日期']).dt.strftime('%Y-%m-%d')
        grouped = supplier_df.groupby('日期')
        
        results = []
        cumulative_denominator = 0
        cumulative_detractor = 0
        cumulative_promoter = 0
        prev_nps = None
        
        for date in sorted(grouped.groups.keys()):
            group = grouped.get_group(date)
            metrics = self._calc_nps_metrics(group)
            
            # 计算累计NPS
            cumulative_denominator += metrics['有效分母']
            cumulative_detractor += metrics['诋毁数']
            cumulative_promoter += metrics['推荐分']
            
            if cumulative_denominator > 0:
                cumulative_nps = ((cumulative_promoter / cumulative_denominator) - 
                                  (cumulative_detractor / cumulative_denominator)) * 100
            else:
                cumulative_nps = 0
            
            # 判断是否进步
            if prev_nps is not None:
                is_improved = '是' if cumulative_nps > prev_nps else ('否' if cumulative_nps < prev_nps else '持平')
            else:
                is_improved = '-'
            
            results.append({
                '日期': date,
                '订单数': metrics['订单数'],
                '有效分母': metrics['有效分母'],
                '诋毁数': metrics['诋毁数'],
                '推荐分': metrics['推荐分'],
                '当日NPS': metrics['NPS'],
                '累计NPS': round(cumulative_nps, 2),
                '是否进步': is_improved
            })
            prev_nps = cumulative_nps
        
        return results
    
    def get_account_dimension(self, supplier_id: int) -> List[Dict]:
        """获取账号维度分析"""
        if self.df is None:
            return []
        
        supplier_df = self.df[self.df['成团供应商id'] == supplier_id]
        if supplier_df.empty:
            return []
        
        # 计算供应商整体NPS
        supplier_metrics = self._calc_nps_metrics(supplier_df)
        supplier_nps = supplier_metrics['NPS']
        
        # 按账号分组
        grouped = supplier_df.groupby(['成团子账号uid', '成团子账号名称'])
        
        results = []
        for (uid, name), group in grouped:
            metrics = self._calc_nps_metrics(group)
            
            # 计算贡献度
            weight = metrics['有效分母'] / supplier_metrics['有效分母'] if supplier_metrics['有效分母'] > 0 else 0
            contribution = (metrics['NPS'] - supplier_nps) * weight
            
            results.append({
                '子账号UID': str(uid),
                '子账号名称': name,
                **metrics,
                '贡献度': round(contribution, 4),
                '负贡献': '是' if metrics['NPS'] < supplier_nps else '否'
            })
        
        # 按NPS降序
        results.sort(key=lambda x: x['NPS'], reverse=True)
        return results
    
    def get_follower_dimension(self, supplier_id: int) -> List[Dict]:
        """获取跟进人维度分析"""
        if self.df is None:
            return []
        
        supplier_df = self.df[self.df['成团供应商id'] == supplier_id]
        if supplier_df.empty:
            return []
        
        # 计算供应商整体NPS
        supplier_metrics = self._calc_nps_metrics(supplier_df)
        supplier_nps = supplier_metrics['NPS']
        
        # 按跟进人分组
        grouped = supplier_df.groupby(['跟进人id', '跟进人姓名'])
        
        results = []
        for (fid, name), group in grouped:
            metrics = self._calc_nps_metrics(group)
            
            # 计算贡献度
            weight = metrics['有效分母'] / supplier_metrics['有效分母'] if supplier_metrics['有效分母'] > 0 else 0
            contribution = (metrics['NPS'] - supplier_nps) * weight
            
            results.append({
                '跟进人ID': str(fid),
                '跟进人姓名': name,
                **metrics,
                '贡献度': round(contribution, 4),
                '负贡献': '是' if metrics['NPS'] < supplier_nps else '否'
            })
        
        # 按NPS降序
        results.sort(key=lambda x: x['NPS'], reverse=True)
        return results
    
    def to_csv(self, data: List[Dict], columns: Optional[List[str]] = None) -> str:
        """将数据转为CSV字符串"""
        if not data:
            return ""
        df = pd.DataFrame(data)
        if columns:
            df = df[columns]
        return df.to_csv(index=False, encoding='utf-8-sig')
    
    def generate_all_csvs(self) -> BytesIO:
        """生成所有CSV打包为ZIP"""
        zip_buffer = BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            # 总体分析
            overall = self.get_overall_analysis()
            if overall:
                columns = ['排名', '供应商ID', '供应商名称', '订单数', '有效分母', '诋毁数', 
                          '推荐分', '诋毁率', '推荐率', 'NPS', '未达目标', '对整体贡献', '负贡献']
                zf.writestr('总体NPS分析.csv', self.to_csv(overall, columns))
            
            # 各供应商分析
            suppliers = self.get_supplier_list()
            for s in suppliers:
                sid = s['id']
                sname = s['name'][:20].replace('/', '_').replace('\\', '_')  # 文件名处理
                
                # 追评管理
                followup = self.get_followup_management(sid)
                if followup:
                    columns = ['订单号', '优先级', '优先级说明', '追评类型', '分母V5', 
                              '是否诋毁', '有用户反馈', '推荐得分']
                    zf.writestr(f'供应商/{sname}/追评管理.csv', self.to_csv(followup, columns))
                
                # 日期维度
                date_dim = self.get_date_dimension(sid)
                if date_dim:
                    columns = ['日期', '订单数', '有效分母', '诋毁数', '推荐分', 
                              '当日NPS', '累计NPS', '是否进步']
                    zf.writestr(f'供应商/{sname}/日期维度.csv', self.to_csv(date_dim, columns))
                
                # 账号维度
                account_dim = self.get_account_dimension(sid)
                if account_dim:
                    columns = ['子账号UID', '子账号名称', '订单数', '有效分母', '诋毁数',
                              '推荐分', '诋毁率', '推荐率', 'NPS', '贡献度', '负贡献']
                    zf.writestr(f'供应商/{sname}/账号维度.csv', self.to_csv(account_dim, columns))
                
                # 跟进人维度
                follower_dim = self.get_follower_dimension(sid)
                if follower_dim:
                    columns = ['跟进人ID', '跟进人姓名', '订单数', '有效分母', '诋毁数',
                              '推荐分', '诋毁率', '推荐率', 'NPS', '贡献度', '负贡献']
                    zf.writestr(f'供应商/{sname}/跟进人维度.csv', self.to_csv(follower_dim, columns))
        
        zip_buffer.seek(0)
        return zip_buffer
