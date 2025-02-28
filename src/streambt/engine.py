
class Engine:

    def __init__(self, backend):
        """
        Initialize the backtesting engine with a strategy and market data.
        
        :param strategy: The trading strategy to be tested.
        :param data: The historical market data.
        """
        self.datafeed = {} #multiple feed from different source
        self.datahold = []
        self.strategy = {}
        self.results = None
    def add_datafeed(self, feed):
        pass
    def add_indicator_to_feed(self, indicator):
        pass
    def add_strategy(self, strategy):
        pass
    def add_broker(self, broker, feed_key:str|list):
        pass
    def __simulate_portfolio(self):
        pass
    def run_backtest(self):
        """
            assume that the vectorized calculation was 
            already done during add, returns the portfolio 
            final return at the end of the period.
            
        """
        yielding = self.__simulate_portfolio()
        return yielding
        #self.results = []
        #for index, row in self.data.iterrows():
        #    signal = self.strategy.generate_signal(row)
        #    self.results.append(signal)
        #return self.results

    def calculate_performance(self):
        """
        Calculate the performance of the strategy based on the backtest results.
        """
        #if self.results is None:
        #    raise ValueError("No results to calculate performance. Run the backtest first.")
        #
        ## Example performance calculation (e.g., cumulative returns)
        #performance = sum(self.results)  # Simplified example
        #return performance

    def plot_results(self):
        """
        Plot the results of the backtest.
        """
        #if self.results is None:
        #    raise ValueError("No results to plot. Run the backtest first.")
        #
        #import matplotlib.pyplot as plt
        #plt.plot(self.results)
        #plt.title('Backtest Results')
        #plt.xlabel('Time')
        #plt.ylabel('Signal')
        #plt.show()