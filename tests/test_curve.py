import csv
import io
import unittest
from unittest.mock import AsyncMock, patch
import httpx
from eth_abi import encode
from fastapi import HTTPException
from routers.curve import CONTROLLER, CURVE_CSV_HEADER, _fetch_curve_rows, _position_row, _read, get_curve_positions_csv

WALLET = '0x' + '12' * 20

def values(collateral=200000000, stablecoin=0, health=202600000000000000):
    return [((collateral, stablecoin, 120000 * 10**18, 4),), (health,),
            ((67700 * 10**18, 65000 * 10**18),), (77500 * 10**18,), (10**9,)]

class CurveTests(unittest.IsolatedAsyncioTestCase):
    def test_units_and_risk(self):
        row = _position_row(WALLET, '0x10', values())
        expected = dict(collateral_amount='2', debt_amount='120000', collateral_value_crvusd='155000', net_value_crvusd='35000', health_percent='20.26', borrow_apr_percent='3.1536', liquidation_range_upper_crvusd='67700', liquidation_range_lower_crvusd='65000', block_number='16')
        for key, value in expected.items():
            self.assertEqual(row[key], value, key)
        self.assertEqual(set(row), set(CURVE_CSV_HEADER))

    def test_soft_liquidation_preserves_converted_collateral(self):
        row = _position_row(WALLET, '0x10', values(100000000, 70000 * 10**18, -10**16))
        for key, value in dict(position_value_crvusd='147500', net_value_crvusd='27500', health_percent='-1', is_soft_liquidation='true', is_liquidatable='true').items():
            self.assertEqual(row[key], value)

    async def test_no_loan_skips_health(self):
        with patch('routers.curve._rpc', AsyncMock(return_value={'result': '0x10'})), patch('routers.curve._read', AsyncMock(return_value=[(False,)])) as read:
            self.assertEqual(await _fetch_curve_rows(None, WALLET), [])
        self.assertEqual(read.await_count, 1)

    async def test_same_block(self):
        with patch('routers.curve._rpc', AsyncMock(return_value={'result': '0x10'})), patch('routers.curve._read', AsyncMock(side_effect=[[(True,)], values()])) as read:
            self.assertEqual(len(await _fetch_curve_rows(None, WALLET)), 1)
        self.assertEqual([call.args[2] for call in read.await_args_list], ['0x10', '0x10'])

    async def test_validation(self):
        for address, chain in [('bad', 1), (WALLET, 8453)]:
            with self.assertRaises(HTTPException) as ctx:
                await get_curve_positions_csv(address, chain)
            self.assertEqual(ctx.exception.status_code, 400)

    async def test_empty_csv(self):
        with patch('routers.curve._fetch_curve_rows', AsyncMock(return_value=[])):
            response = await get_curve_positions_csv(WALLET, 1)
        self.assertEqual(list(csv.reader(io.StringIO(response.body.decode()))), [CURVE_CSV_HEADER])

    async def test_batch_order(self):
        calls = [(CONTROLLER, 'rate()', [], [], ['uint256'])] * 2
        results = [{'id': i, 'result': '0x' + encode(['uint256'], [i + 10]).hex()} for i in [1, 0]]
        with patch('routers.curve._rpc', AsyncMock(return_value=results)):
            self.assertEqual(await _read(None, 'unused', '0x10', calls), [(10,), (11,)])

    async def test_malformed_results(self):
        calls = [(CONTROLLER, 'rate()', [], [], ['uint256'])]
        for result in [[{'id': 0, 'error': {'message': 'revert'}}], [{'id': 0, 'result': '0x01'}], [], {}]:
            with patch('routers.curve._rpc', AsyncMock(return_value=result)):
                with self.assertRaises(HTTPException) as ctx:
                    await _read(None, 'unused', '0x10', calls)
                self.assertEqual(ctx.exception.status_code, 502)

    async def test_transport_failure(self):
        async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(503))) as client:
            with self.assertRaises(HTTPException) as ctx:
                await _fetch_curve_rows(client, WALLET)
        self.assertEqual(ctx.exception.status_code, 502)
