import backtrader as bt
import sqlite3
import pandas as pd
import datetime
import argparse

# 투자전략 만들기
class TestStrategy(bt.Strategy):
    params = (
        ('exitbars', 5),
        ('maperiod', 15),
        ('printlog', False),
    )

    def log(self, txt, dt=None, doprint=False):
        '''전략의 로그 남기기'''
        # dt = dt or self.datas[0].datetime.date(0)
        # print('%s, %s' % (dt.isoformat(), txt))
        if self.params.printlog or doprint:
            dt = dt or self.datas[0].datetime.date(0)
            print('%s, %s' % (dt.isoformat(), txt))

    def __init__(self):
        # 변수 모음 ==============================================================================
        self.dataclose = self.datas[0].close
        self.dataopen = self.datas[0].open

        self.order = None
        self.buyprice = None
        self.buycomm = None
        # ======================================================================================

        # 지표 추가 ========================================================================================
        self.sma = bt.indicators.MovingAverageSimple(self.datas[0], period=self.params.maperiod)

        # bt.indicators.ExponentialMovingAverage(self.datas[0], period=25)
        # bt.indicators.Stochastic(self.datas[0])
        # rsi = bt.indicators.RSI(self.datas[0])
        # bt.indicators.SmoothedMovingAverage(rsi, period=10)
        # bt.indicators.ATR(self.datas[0], plot=False)
        # =================================================================================================

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            return

        # 주문 완료됐는지 체크
        if order.status in [order.Completed]:
            if order.isbuy():
                self.log('매수 체결| 매수가: %.2f, 비용: %.2f, 수수료: %.2f' %
                         (order.executed.price, order.executed.value, order.executed.comm), doprint=True)
            elif order.issell():
                self.log('매도 체결| 매도가: %.2f, 비용: %.2f, 수수료: %.2f' %
                         (order.executed.price, order.executed.value, order.executed.comm), doprint=True)

            self.bar_executed = len(self)

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('주문 취소/거절', doprint=True)

        self.order = None

    def notify_trade(self, trade):
        if not trade.isclosed:
            return

        self.log('총손익: %.2f, 순손익: %.2f' %
                 (trade.pnl, trade.pnlcomm), doprint=True)

    def next(self):
        self.log('시가: %.2f 종가: %.2f' % (self.dataopen[0], self.dataclose[0]), doprint=True)

        # 진행 중인 주문이 있는지 확인
        if self.order:
            return

        # 포지션 보유 중인지 확인
        if not self.position:
            # 3일 연속 종가 하락하면 Buy 시그널 =======================================================================================
            ## 직전 일자 데이터 없으면 가장 최근데이터 가져옴 >> data length 확인 필요
            if len(self) > 3:
                if self.dataclose[0] < self.dataclose[-1]:
                    if self.dataclose[-1] < self.dataclose[-2]:
                        self.log('매수 시그널: %.2f (전일종가: %.2f | 2일전종가: %.2f)' %
                                 (self.dataclose[0], self.dataclose[-1], self.dataclose[-2]), doprint=True)
                        self.order = self.buy()

            # 이동평균선 지표 시그널 =================================================================================================
            # if self.dataclose[0] > self.sma[0]:
            #     self.log('매수 시그널: %.2f' % self.dataclose[0])
            #     self.order = self.buy()
        else:
            # 보유 후 N일 경과하면 매도
            if len(self) >= (self.bar_executed + self.params.exitbars):
                self.log('매도 시그널: %.2f' % self.dataclose[0], doprint=True)

            # 이동평균선 지표 시그널 ==================================================================================================
            # if self.dataclose[0] > self.sma[0]:
            #     self.log('매도 시그널: %.2f' % self.dataclose[0])
            #
            #     self.order = self.sell()

    # def stop(self):
    #     self.log('%2d 이동평균선 기말 포트폴리오 가치 %.2f' %
    #              (self.params.maperiod, self.broker.getvalue()), doprint=True)



if __name__ == '__main__':
    cerebro = bt.Cerebro()

    # 전략 추가 ===================================================================================
    cerebro.addstrategy(TestStrategy)

    # 데이터 가져오기 ===============================================================================
    from_date = "20200801"
    to_date = ''
    data_path = 'E:/DB/cmp_ohlcv.db'
    ## 시총 상위 200개 종목
    sql = "SELECT * FROM cmp_master WHERE Cmp_type=1 AND List_date < " + from_date + " ORDER BY Mkt_cap DESC LIMIT 200"
    con = sqlite3.connect(data_path)
    code_list = pd.read_sql(sql, con)
    code_list = code_list['Code']
    code_list = code_list[:4]
    # ============================================================================================

    # 데이터 Cerebro에 입력 =========================================================================
    ## 여러종목 입력 시 문제점 >> 최근 상장한 종목 있을 경우, 해당 종목의 상장일부터 전략 테스트 돌기 시작함
    ## ex) 빅히트 상장: 20201015 >> sql 조건문에 day>'20200101' 넣어도 테스트는 20201015부터.. >> 상장일 함께 체크
    # sql = "SELECT * FROM '{0}' WHERE day > " + from_date + " ORDER BY day ASC"
    sql = "SELECT DAY, OPEN, HIGH, LOW, CLOSE, VOLUME  FROM T_STK_DATA WHERE CMP_CD = '{0}' AND DAY > " + from_date + " ORDER BY DAY ASC"
    for i, code in enumerate(code_list):
        if i == 0:
            data = pd.read_sql(sql.format(code), con, index_col='DAY', parse_dates=['DAY'])
            data = bt.feeds.PandasData(dataname=data, name=code)
            cerebro.adddata(data)
        else:
            data_n = pd.read_sql(sql.format(code), con, index_col='DAY', parse_dates=['DAY'])
            data_n = bt.feeds.PandasData(dataname=data_n, name=code)
            data_n.plotinfo.plotmaster = data
            cerebro.adddata(data_n)

    # code = 'A005930'
    # data = pd.read_sql(sql.format(code), con, index_col='DAY', parse_dates=['DAY'])
    # data = bt.feeds.PandasData(dataname=data, name=code)
    # cerebro.adddata(data)
    # ============================================================================================

    # 초기현금, 수수료, Sizer 세팅 ===================================================================
    cerebro.broker.set_cash(10000000)
    cerebro.broker.setcommission(0.015)
    cerebro.addsizer(bt.sizers.FixedSize, stake=10)
    # ============================================================================================

    print('기초 포트폴리오 가치: %.2f' % cerebro.broker.getvalue())

    cerebro.run()

    print('기말 포트폴리오 가치: %.2f' % cerebro.broker.getvalue())

    cerebro.plot()