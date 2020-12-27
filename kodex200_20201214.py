from __future__ import (absolute_import, division, print_function, unicode_literals)

import datetime
import pandas as pd
import sqlite3

import backtrader as bt

class Multi_Securities(bt.Strategy):
    params = dict(
        drate=5,
        num_dates=2,
    )

    def log(self, txt, dt=None):
        '''전략의 로그 남기기'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):

        self.inds = dict()
        for i, d in enumerate(self.datas):
            self.inds[d] = dict()
            self.inds[d]['close'] = d.close
            self.inds[d]['open'] = d.open
            self.inds[d]['volume'] = d.volume

            self.inds[d]['ma_5'] = bt.indicators.MovingAverageSimple(d.close, period=5)
            self.inds[d]['ma_20'] = bt.indicators.MovingAverageSimple(d.close, period=20)

    def next(self):
        for i, d in enumerate(self.datas):
            dt, dn = self.datetime.date(), d._name
            pos = self.getposition(d).size
            self.log('{} 시가: {} 종가: {}'.format(dn, self.inds[d]['open'][0], self.inds[d]['close'][0]))

            # 1. 5일선 추세 3일간 상승반전 후 20일선 돌파
            if len(self) > self.p.num_dates:
                if not pos:
                    if self.inds[d]['close'][0] > self.inds[d]['close'][-1]:
                        if self.inds[d]['close'][-1] > self.inds[d]['close'][-2]:
                            if self.inds[d]['ma_5'] > self.inds[d]['ma_20']:
                                if self.inds[d]['open'][0] != self.inds[d]['close'][0]:
                                    print(self.inds[d]['close'][0], self.inds[d]['close'][-1], self.inds[d]['close'][-2])
                                    self.order = self.order_target_percent(data=d, target=0.99)

                # 익일 시초 매도
                else:
                    self.close(data=d)

    # def next_open(self):
    #     for i, d in enumerate(self.datas):
    #         self.close(data=d)


    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('{} 총손익: {} | 순손익: {}'.format(trade.data._name, round(trade.pnl, 2), round(trade.pnlcomm, 2)))

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        # 주문 완료됐는지 체크
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('%s >> 매수 체결| 매수가: %.2f, 비용: %.2f, 수수료: %.2f' %
                         (order.data._name, order.executed.price, order.executed.value, order.executed.comm))
            elif order.issell():
                self.log('%s >> 매도 체결| 매도가: %.2f, 비용: %.2f, 수수료: %.2f' %
                         (order.data._name, order.executed.price, order.executed.value, order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('주문 취소/거절')

        self.order = None


def printTradeAnalysis(analyzer):
    '''
    Function to print the Technical Analysis results in a nice format.
    '''
    #Get the results we are interested in
    total_open = analyzer.total.open
    total_closed = analyzer.total.closed
    total_won = analyzer.won.total
    total_lost = analyzer.lost.total
    win_streak = analyzer.streak.won.longest
    lose_streak = analyzer.streak.lost.longest
    pnl_net = format(int(round(analyzer.pnl.net.total, 2)), ',d')

    strike_rate = round(((total_won / total_closed) * 100), 2)
    #Designate the rows
    h1 = ['Total Open', 'Total Closed', 'Total Won', 'Total Lost']
    h2 = ['Strike Rate','Win Streak', 'Losing Streak', 'PnL Net']
    r1 = [total_open, total_closed,total_won,total_lost]
    r2 = [strike_rate, win_streak, lose_streak, pnl_net]
    #Check which set of headers is the longest.
    if len(h1) > len(h2):
        header_length = len(h1)
    else:
        header_length = len(h2)
    #Print the rows
    print_list = [h1,r1,h2,r2]
    row_format ="{:<15}" * (header_length + 1)
    print("Trade Analysis Results:")
    for row in print_list:
        print(row_format.format('',*row))

def printSQN(analyzer):
    sqn = round(analyzer.sqn, 2)
    if sqn > 7.0:
        text = '전략 점수 Too high >> 오류 없나 확인 필요'
    elif sqn > 5.1:
        text = '전략 점수 Super Good..!!'
    elif sqn > 3.0:
        text = '전략 점수 Excellent!!'
    elif sqn > 2.5:
        text = '전략 점수 Good!'
    elif sqn > 2.0:
        text = '전략 점수 Average'
    elif sqn > 1.6:
        text = '전략 점수 Below Avg'
    else:
        text = '전략 점수 너무 낮음 >> 쓰레기..'
    print('SQN: {} >> '.format(sqn) + text)


def runstrat():
    cerebro = bt.Cerebro()

    cerebro.addstrategy(Multi_Securities)

    # 데이터 가져오기 ===============================================================================
    from_date = "20200101"
    to_date = ''
    data_path = 'E:/DB/cmp_ohlcv.db'
    ## 시총 상위 200개 종목
    sql = "SELECT * FROM cmp_master WHERE Cmp_type=1 AND List_date < " + from_date + " ORDER BY Mkt_cap DESC LIMIT 200"
    con = sqlite3.connect(data_path)
    code_list = pd.read_sql(sql, con)
    # ============================================================================================

    # 데이터 Cerebro에 입력 =========================================================================
    sql = "SELECT DAY, OPEN, HIGH, LOW, CLOSE, VOLUME  FROM T_STK_DATA WHERE CMP_CD = '{0}' AND DAY > " + from_date + " ORDER BY DAY ASC"

    code = 'A069500'
    data = pd.read_sql(sql.format(code), con, index_col='DAY', parse_dates=['DAY'])
    data = bt.feeds.PandasData(dataname=data, name=code)
    # data.addfilter(bt.filters.DaySplitter_Close)
    data.addfilter(bt.filters.BarReplayer_Open)
    cerebro.adddata(data)

    # Broker
    cerebro.broker.setcommission(commission=0.0015)
    cerebro.broker.set_cash(100000000)
    cerebro.broker.set_coc(True)
    # cerebro.broker.set_coo(True)

    # Analyzer ===============================================================================================
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='ta')
    cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn')

    # Sizer ==================================================================================================
    cerebro.addsizer(bt.sizers.AllInSizerInt)


    print('기초 포트폴리오: %.2f' % cerebro.broker.getvalue())

    strat = cerebro.run(maxcpus=1)
    firstStrat = strat[0]

    print('기말 포트폴리오: %.2f' % cerebro.broker.getvalue())

    # printTradeAnalysis(firstStrat.analyzers.ta.get_analysis())
    # printSQN(firstStrat.analyzers.sqn.get_analysis())

    cerebro.plot()

if __name__ == '__main__':
    runstrat()