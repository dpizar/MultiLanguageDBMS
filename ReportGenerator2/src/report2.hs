import System.IO  
import Control.Monad
import Data.List
import Text.Printf

c_cust::(a,b,c,d,e) -> a
c_cust (a,_,_,_,_) = a

c_name::(a,b,c,d,e) -> b
c_name (_,b,_,_,_) = b

c_age::(a,b,c,d,e) -> c
c_age (_,_,c,_,_) = c

c_phone::(a,b,c,d,e) -> d
c_phone (_,_,_,d,_) = d

c_address::(a,b,c,d,e) -> e
c_address (_,_,_,_,e) = e

s_order::(a,b,c,d) -> a
s_order (a,_,_,_) = a

s_cust::(a,b,c,d) -> b
s_cust (_,b,_,_) = b

s_date::(a,b,c,d) -> c
s_date (_,_,c,_) = c

s_total::(a,b,c,d) -> d
s_total (_,_,_,d) = d

o_order::(a,b) -> a
o_order (a,_) = a

o_item::(a,b) -> b
o_item (_,b) = b

split :: String -> Char -> [String]
split [] delim = [""]
split (c:cs) delim
   | c == delim = "" : rest
   | otherwise = (c : head rest) : tail rest
   where
       rest = split cs delim

custDB :: [String] -> [(Int,[Char],Int,[Char],[Char])]
salesDB :: [String] -> [(Int, Int, String, Float)]
orderDB :: [String] -> [(Int, String)]

salesOrderDB :: String -> [(Int, Int, String, Float)] -> [(Int, String)] -> [((String, Int, String, Float),Int)]

custSalesDB :: [((String, Int, String, Float),Int)] -> [(Int,[Char],Int,[Char],[Char])] -> [(((String, Int, String, Float),Int),(Int,[Char],Int,[Char],[Char]))]

salesSortf :: (Int, Int, String, Float) -> (Int, Int, String, Float) -> Ordering
orderSales :: [(Int, Int, String, Float)] -> [(Int, Int, String, Float)]

orderSortf :: (Int, String) -> (Int, String) -> Ordering
sortOrders :: [(Int, String)] -> [(Int, String)]

csalesSortf :: ((String, Int, String, Float),Int) -> ((String, Int, String, Float),Int) -> Ordering
sortcSales :: [((String, Int, String, Float),Int)] -> [((String, Int, String, Float),Int)]

custSortf :: (Int,[Char],Int,[Char],[Char]) -> (Int,[Char],Int,[Char],[Char]) -> Ordering
sortCusts :: [(Int,[Char],Int,[Char],[Char])] -> [(Int,[Char],Int,[Char],[Char])]

printOrderSales :: [(((String, Int, String, Float),Int),(Int,[Char],Int,[Char],[Char]))] -> IO()
--printOrderSales :: [(String, Int, String, Float)] -> IO()

custDB [] = []

custDB (x:xs) = do
    let items = split x '|'
    (read (items!!0)::Int, items!!1, read (items!!2)::Int, items!!3, items!!4):custDB xs

salesDB [] = []

salesDB (x:xs) = do
    let items = split x '|'
    (read (items!!0)::Int, read (items!!1)::Int, items!!2, read (items!!3)::Float):salesDB xs
    
orderDB [] = []

orderDB (x:xs) = do
    let items = split x '|'
    (read (items!!0)::Int, items!!1):orderDB xs

salesOrderDB s [] [] = []

salesOrderDB s [] a = []

salesOrderDB s (x:xs) [] = do
    if( s=="" ) then
        []
    else
        ((s, s_cust x, s_date x, s_total x), s_order x):[]

salesOrderDB s (x:xs) (y:ys) = do
    if( (s_order x)==(o_order y) ) then do
        if( s=="" ) then do
            salesOrderDB (o_item y) (x:xs) ys
        else do
            salesOrderDB (s++", "++(o_item y)) (x:xs) ys
    else do
        ((s, s_cust x, s_date x, s_total x), s_order x):salesOrderDB "" xs (y:ys)

custSalesDB [] [] = []

custSalesDB [] a = []

custSalesDB (x:xs) [] = []

custSalesDB (x:xs) (y:ys) = do
    if( (s_cust (fst x))==(c_cust y) ) then do
        (x,y):custSalesDB xs (y:ys)
    else do
        custSalesDB (x:xs) ys

salesSortf a b
    | s_order a < s_order b = LT
    | otherwise = GT

orderSortf a b
    | o_order a < o_order b = LT
    | otherwise = GT
    
csalesSortf a b
    | s_cust (fst a) < s_cust (fst b) = LT
    | otherwise = GT
    
custSortf a b
    | c_cust a < c_cust b = LT
    | otherwise = GT

orderSales [] = []

orderSales a = do
    sortBy salesSortf a
    
sortOrders [] = []

sortOrders a = do
    sortBy orderSortf a
    
sortcSales [] = []

sortcSales a = do
    sortBy csalesSortf a
    
sortCusts a = do
    sortBy custSortf a

printOrderSales [] = do
    return ()

printOrderSales (x:xs) = do
    let s = fst x
    let c = snd x
    let line = printf "%s\t%d\t%s\t%s" (c_name c) (snd s) (s_date (fst s)) (s_order (fst s))
    --let line = printf "%d" (s_cust x)
    putStrLn line
    printOrderSales xs

main = do
    sales <- readFile "SalesDB.apd"
    custs <- readFile "CustDB.apd"
    orders <- readFile "OrderDB.apd"

    let saleslines = lines sales
    let custslines = lines custs
    let orderlines = lines orders
    let sDB = orderSales (salesDB saleslines)
    let oDB = sortOrders (orderDB orderlines)
    let osDB = sortcSales (salesOrderDB "" sDB oDB)
    
    let cDB =  sortCusts (custDB custslines)
    let final = custSalesDB osDB cDB

    let title = "Cust\tSales"
    putStrLn title
    printOrderSales final