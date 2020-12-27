from __future__ import (absolute_import, division, print_function, unicode_literals)

import datetime
import pandas as pd
import sqlite3

import backtrader as bt
# import pyfolio as pf

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

            self.cmp_list = dict()

    def next(self):
        # 종목 개수
        cmp_cnt = 0
        day = self.datetime.date()
        for i, d in enumerate(self.datas):
            dt, dn = self.datetime.date(), d._name
            pos = self.getposition(d).size
            # self.log('{} 시가: {} 종가: {}'.format(dn, self.inds[d]['open'][0], self.inds[d]['close'][0]))

            # 1. 5일선 추세 3일간 상승반전 후 20일선 돌파
            if len(self) > self.p.num_dates:
                if not pos:
                    if self.inds[d]['close'][0] > self.inds[d]['close'][-1]:
                        if self.inds[d]['close'][-1] > self.inds[d]['close'][-2]:
                            if self.inds[d]['ma_5'] > self.inds[d]['ma_20']:
                                cmp_cnt += 1

                                if dt in self.cmp_list.keys():
                                    pass
                                else:
                                    self.cmp_list[dt] = []

                                self.cmp_list[dt].append(dn)

            # # 2. 이격도(엔벨로프) 상승추세 종목
            # if len(self) > self.p.num_dates:
            #     if not pos:
            #         if self.inds[d]['volume'] > self.inds[d]['volume']:
            #             cmp_cnt += 1
            #
            #             if dt in self.cmp_list.keys():
            #                 pass
            #             else:
            #                 self.cmp_list[dt] = []
            #
            #             self.cmp_list[dt].append(dn)

                # 익일 시초 매도
                else:
                    self.close(data=d)

        if cmp_cnt != 0:
            for i, d in enumerate(self.cmp_list[dt]):
                if cmp_cnt < 10:
                    equal_weight = 0.1
                else:
                    equal_weight = float(1 / cmp_cnt) - 0.002
                self.order = self.order_target_percent(data=d, target=equal_weight)

        print('날짜: %s, 종목수: %s, PF Value: %s' % (day, cmp_cnt, self.broker.getvalue()))
        try:
            print(self.cmp_list[day])
        except:
            pass

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
            self.log('%s >> 주문 취소/거절' % order.data._name)

        self.order = None

    # def stop(self):
    #     self.log('전일대비 %s%% 하락주 투자전략: %.2f' % (self.p.drate, self.broker.getvalue()))

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
    pnl_net = format(int(round(analyzer.pnl.net.total,2)), ',d')

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

def printReturn(analyzer):
    pass

def printDrawDown(analyzer):
    drawdown = round(analyzer.drawdown,2)
    moneydown = round(analyzer.moneydown,2)
    duration = analyzer.len
    maxdrawdown = round(analyzer.max.drawdown,2)
    maxmoneydown = round(analyzer.max.moneydown,2)
    maxduration = analyzer.max.len
    print('Drawdown %    : {}, Drawdown $    : {}, Duration    : {}'.format(drawdown,moneydown,duration))
    print('Max Drawdown %: {}, Max Drawdown $: {}, Max Duration: {}'.format(maxdrawdown,maxmoneydown,maxduration))

def runstrat():
    cerebro = bt.Cerebro()

    # 전략 추가 or 최적화===================================================================================
    cerebro.addstrategy(Multi_Securities)
    # cerebro.optstrategy(Multi_Securities, drate=range(10, 30))


    # 데이터 가져오기 ===============================================================================
    from_date = "20200101"
    to_date = ''
    data_path = 'E:/DB/cmp_ohlcv.db'
    ## 시총 상위 200개 종목
    sql = "SELECT * FROM cmp_master WHERE Cmp_type=1 AND List_date < " + from_date + " ORDER BY Mkt_cap DESC LIMIT 200"
    con = sqlite3.connect(data_path)
    code_list = pd.read_sql(sql, con)
    code_list = code_list['Code']
    # code_list = code_list[:3]
    # ============================================================================================

    # 데이터 Cerebro에 입력 =========================================================================
    ## 여러종목 입력 시 문제점 >> 최근 상장한 종목 있을 경우, 해당 종목의 상장일부터 전략 테스트 돌기 시작함
    ## ex) 빅히트 상장: 20201015 >> sql 조건문에 day>'20200101' 넣어도 테스트는 20201015부터.. >> 상장일 함께 체크
    # sql = "SELECT * FROM '{0}' WHERE day > " + from_date + " ORDER BY day ASC"
    sql = "SELECT DAY, OPEN, HIGH, LOW, CLOSE, VOLUME  FROM T_STK_DATA WHERE CMP_CD = '{0}' AND DAY > " + from_date + " ORDER BY DAY ASC"
    for i, code in enumerate(code_list):
        if i == 0:
            data = pd.read_sql(sql.format(code), con, index_col='DAY', parse_dates=['DAY'])
            data = bt.feeds.PandasData(dataname=data, name=code, plot=False)        # plot=False >> 차트에 개별 종목 데이터 미출력
            cerebro.adddata(data)
        else:
            data_n = pd.read_sql(sql.format(code), con, index_col='DAY', parse_dates=['DAY'])
            data_n = bt.feeds.PandasData(dataname=data_n, name=code, plot=False)
            data_n.plotinfo.plotmaster = data
            cerebro.adddata(data_n)

    # Broker ==================================================================================================
    cerebro.broker.setcommission(commission=0.0015)
    cerebro.broker.set_cash(100000000)
    cerebro.broker.set_coc(True)

    # Analyzer ===============================================================================================
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='ta')
    cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn')
    cerebro.addanalyzer(bt.analyzers.Returns, _name='returns')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

    print('기초 포트폴리오: %.2f' % cerebro.broker.getvalue())

    strat = cerebro.run(maxcpus=1)
    firstStrat = strat[0]

    print('기말 포트폴리오: %.2f' % cerebro.broker.getvalue())

    printTradeAnalysis(firstStrat.analyzers.ta.get_analysis())
    printSQN(firstStrat.analyzers.sqn.get_analysis())
    # printReturn(firstStrat.returns.get_analysis())
    printDrawDown(firstStrat.analyzers.drawdown.get_analysis())

    cerebro.plot()

if __name__ == '__main__':
    runstrat()