import time
from typing import Dict
from websocket_proxy_old.ring_buffer import LockFreeRingBuffer
from websocket_proxy_old.market_data import MarketDataMessage

class SharedMemoryPublisher:
    def __init__(self, buffer_name: str = "ws_proxy_buffer"):
        self.buffer_name = buffer_name
        self.ring_buffer = None
        self.running = False
        self.stats = {
            'messages_published': 0,
            'publish_failures': 0,
            'avg_latency_ns': 0,
            'initialization_error': None
        }
        
        try:
            self.ring_buffer = LockFreeRingBuffer(buffer_name, create=True)
        except Exception as e:
            # Fallback for race conditions: try opening existing segment after a short delay
            error_msg = f"Failed to initialize shared memory: {e}"
            print(error_msg)
            self.stats['initialization_error'] = error_msg
            try:
                time.sleep(0.05)
                self.ring_buffer = LockFreeRingBuffer(buffer_name, create=False)
                # Clear the initialization error on successful fallback
                self.stats['initialization_error'] = None
            except Exception as e2:
                print(f"Fallback open existing shared memory failed: {e2}")
    
    def publish_market_data(self, symbol: str, data: dict) -> bool:
        """Publish market data with nanosecond timestamp"""
        if self.ring_buffer is None:
            print("Error: Ring buffer not initialized")
            self.stats['publish_failures'] += 1
            return False
            
        start_time = time.time_ns()
        
        try:
            # Map incoming dict to MarketDataMessage fields
            ts_sec = start_time / 1_000_000_000  # seconds
            price = data.get('ltp', data.get('price', 0))
            bid = data.get('bid', data.get('best_bid_price', 0))
            ask = data.get('ask', data.get('best_ask_price', 0))
            vol = data.get('volume', data.get('qty', 0))
            bid_qty = data.get('bid_qty', data.get('best_bid_qty', 0))
            ask_qty = data.get('ask_qty', data.get('best_ask_qty', 0))
            # OHLC and related fields (support multiple possible keys)
            open_px = data.get('open', data.get('open_price', data.get('o', 0)))
            high_px = data.get('high', data.get('high_price', data.get('h', 0)))
            low_px = data.get('low', data.get('low_price', data.get('l', 0)))
            close_px = data.get('close', data.get('close_price', data.get('c', 0)))
            avg_px = data.get('avg_price', data.get('average_price', data.get('avg', 0)))
            pct_chg = data.get('percent_change', data.get('change_percent', data.get('chg_perc', 0)))
            last_qty = data.get('last_qty', data.get('last_quantity', data.get('last_trade_qty', 0)))
            # Determine message type: prefer 'type' string, fallback to numeric 'mode'
            msg_type = data.get('type', data.get('mode'))
            # Normalize message type
            if isinstance(msg_type, str):
                mt = {'LTP': 1, 'QUOTE': 2, 'DEPTH': 3}.get(msg_type.upper(), 1)
            else:
                mt = int(msg_type) if msg_type is not None else 1

            # Depth totals
            total_buy_qty = data.get('total_buy_quantity', 0) or 0
            total_sell_qty = data.get('total_sell_quantity', 0) or 0

            # Depth arrays (expecting normalized format: data['depth'] with buy/sell arrays)
            depth = data.get('depth') or {}
            buy_levels = depth.get('buy') or []
            sell_levels = depth.get('sell') or []

            # Helper to read level dict safely
            def lvl(arr, idx):
                if idx < len(arr) and isinstance(arr[idx], dict):
                    d = arr[idx]
                    return (
                        float(d.get('price') or 0),
                        float(d.get('quantity') or 0),
                        float(d.get('orders') or 0),
                    )
                return (0.0, 0.0, 0.0)

            bp1, bq1, bo1 = lvl(buy_levels, 0)
            bp2, bq2, bo2 = lvl(buy_levels, 1)
            bp3, bq3, bo3 = lvl(buy_levels, 2)
            bp4, bq4, bo4 = lvl(buy_levels, 3)
            bp5, bq5, bo5 = lvl(buy_levels, 4)

            ap1, aq1, ao1 = lvl(sell_levels, 0)
            ap2, aq2, ao2 = lvl(sell_levels, 1)
            ap3, aq3, ao3 = lvl(sell_levels, 2)
            ap4, aq4, ao4 = lvl(sell_levels, 3)
            ap5, aq5, ao5 = lvl(sell_levels, 4)

            message = MarketDataMessage(
                symbol=str(symbol),
                timestamp=float(ts_sec),
                price=float(price or 0),
                volume=float(vol or 0),
                bid_price=float(bid or 0),
                ask_price=float(ask or 0),
                bid_quantity=float(bid_qty or 0),
                ask_quantity=float(ask_qty or 0),
                open_price=float(open_px or 0),
                high_price=float(high_px or 0),
                low_price=float(low_px or 0),
                close_price=float(close_px or 0),
                average_price=float(avg_px or 0),
                percent_change=float(pct_chg or 0),
                last_quantity=float(last_qty or 0),
                total_buy_quantity=float(total_buy_qty),
                total_sell_quantity=float(total_sell_qty),
                # Bid depth
                bid_price_1=bp1, bid_qty_1=bq1, bid_orders_1=bo1,
                bid_price_2=bp2, bid_qty_2=bq2, bid_orders_2=bo2,
                bid_price_3=bp3, bid_qty_3=bq3, bid_orders_3=bo3,
                bid_price_4=bp4, bid_qty_4=bq4, bid_orders_4=bo4,
                bid_price_5=bp5, bid_qty_5=bq5, bid_orders_5=bo5,
                # Ask depth
                ask_price_1=ap1, ask_qty_1=aq1, ask_orders_1=ao1,
                ask_price_2=ap2, ask_qty_2=aq2, ask_orders_2=ao2,
                ask_price_3=ap3, ask_qty_3=aq3, ask_orders_3=ao3,
                ask_price_4=ap4, ask_qty_4=aq4, ask_orders_4=ao4,
                ask_price_5=ap5, ask_qty_5=aq5, ask_orders_5=ao5,
                message_type=mt
            )
            
            success = self.ring_buffer.publish(message)
            
            # Update statistics
            end_time = time.time_ns()
            if success:
                self.stats['messages_published'] += 1
                latency = end_time - start_time
                # Running average
                if self.stats['messages_published'] > 1:
                    self.stats['avg_latency_ns'] = (
                        (self.stats['avg_latency_ns'] * (self.stats['messages_published'] - 1) + latency) 
                        / self.stats['messages_published']
                    )
                else:
                    self.stats['avg_latency_ns'] = latency
            else:
                self.stats['publish_failures'] += 1
                print("Failed to publish message: buffer full or error occurred")
            
            return success
            
        except Exception as e:
            print(f"Error in publish_market_data: {e}")
            self.stats['publish_failures'] += 1
            return False
    
    def get_stats(self) -> dict:
        return self.stats.copy()
    
    def start(self):
        self.running = True
    
    def stop(self):
        self.running = False
        if hasattr(self, 'ring_buffer') and self.ring_buffer is not None:
            try:
                self.ring_buffer.close()
            except Exception as e:
                print(f"Error stopping publisher: {e}")
    
    def cleanup(self):
        if hasattr(self, 'ring_buffer') and self.ring_buffer is not None:
            try:
                self.ring_buffer.unlink()
            except Exception as e:
                print(f"Error cleaning up publisher: {e}")
