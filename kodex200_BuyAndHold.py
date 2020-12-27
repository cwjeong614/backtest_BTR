import sqlite3
import pandas as pd
import backtrader as bt
import math

class Buy_and_Hold(bt.Strategy):
    params = dict(
        drate=5,
        num_dates=2,
        use_target_percent=False,
    )

    def log(self, txt, dt=None):
        '''전략의 로그 남기기'''
        dt = dt or self.datas[0].datetime.date(0)
        print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open

        self.order = None
        self.buyprice = None
        self.buycomm = None

        self.ma_5= bt.indicators.MovingAverageSimple(self.dataclose, period=5)
        self.ma_20 = bt.indicators.MovingAverageSimple(self.dataclose, period=20)

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        # self.log('{} 총손익: {} | 순손익: {}'.format(trade.data._name, round(trade.pnl, 2), round(trade.pnlcomm, 2)))
        self.log('{} 총손익: {} | 순손익: {}'.format(trade.data._name, format(int(trade.pnl), ',d'), format(int(trade.pnlcomm), ',d')))

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        # 주문 완료됐는지 체크
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('%s >> 매수 체결| 매수가: %.2f, 비용: %.2f, 포지션사이즈: %s, 수수료: %.2f' %
                         (order.data._name, order.executed.price, order.executed.value, self.position.size, order.executed.comm))
            elif order.issell():
                self.log('%s >> 매도 체결| 매도가: %.2f, 비용: %.2f, 수수료: %.2f' %
                         (order.data._name, order.executed.price, order.executed.value, order.executed.comm))

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('주문 취소/거절')

        self.order = None


    # buy&hold
    def next(self):
        if not self.position:
            self.order = self.order_target_percent(target=0.99)


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

    cerebro.addstrategy(Buy_and_Hold)

    # 데이터 가져오기 ===============================================================================
    from_date = "20200101"
    data_path = 'E:/DB/cmp_ohlcv.db'
    ## 시총 상위 200개 종목
    sql = "SELECT * FROM cmp_master WHERE Cmp_type=1 AND List_date < " + from_date + " ORDER BY Mkt_cap DESC LIMIT 200"
    con = sqlite3.connect(data_path)
    code_list = pd.read_sql(sql, con)
    code_list = code_list['Code']
    # code_list = code_list[:3]
    # ============================================================================================

    # 데이터 Cerebro에 입력 =========================================================================
    sql = "SELECT DAY, OPEN, HIGH, LOW, CLOSE, VOLUME  FROM T_STK_DATA WHERE CMP_CD = '{0}' AND DAY > " + from_date + " ORDER BY DAY ASC"

    code = 'A069500'
    data = pd.read_sql(sql.format(code), con, index_col='DAY', parse_dates=['DAY'])
    data = bt.feeds.PandasData(dataname=data, name=code)
    cerebro.adddata(data)

    # Broker ================================================================================================
    cerebro.broker.setcommission(commission=0.0015)
    cerebro.broker.set_cash(100000000)
    cerebro.broker.set_coc(True)

    # Analyzer ===============================================================================================
    cerebro.addanalyzer(bt.analyzers.TradeAnalyzer, _name='ta')
    cerebro.addanalyzer(bt.analyzers.SQN, _name='sqn')
    cerebro.addanalyzer(bt.analyzers.DrawDown, _name='drawdown')

    # Sizer =================================================================================================
    cerebro.addsizer(bt.sizers.AllInSizerInt)

    print('기초 포트폴리오: %.2f' % cerebro.broker.getvalue())

    strat = cerebro.run(maxcpus=1)
    firstStrat = strat[0]

    print('기말 포트폴리오: %.2f' % cerebro.broker.getvalue())

    # printTradeAnalysis(firstStrat.analyzers.ta.get_analysis())
    # printSQN(firstStrat.analyzers.sqn.get_analysis())
    # printDrawDown(firstStrat.analyzers.drawdown.get_analysis())
    cerebro.plot()

if __name__ == '__main__':
    runstrat()