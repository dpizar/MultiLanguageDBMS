import System.IO  
import Control.Monad
import Data.List
import Text.Printf

s_order::(a,b,c,d) -> a
s_order (a,_,_,_) = a

s_cust::(a,b,c,d) -> b
s_cust (_,b,_,_) = b

s_date::(a,b,c,d) -> c
s_date (_,_,c,_) = c

s_total::(a,b,c,d) -> d
s_total (_,_,_,d) = d

split :: String -> Char -> [String]
split [] delim = [""]
split (c:cs) delim
   | c == delim = "" : rest
   | otherwise = (c : head rest) : tail rest
   where
       rest = split cs delim

salesDB :: [String] -> [(Int, Int, String, Float)]
salesSortf :: (Int, Int, String, Float) -> (Int, Int, String, Float) -> Ordering
orderSales :: [(Int, Int, String, Float)] -> [(Int, Int, String, Float)]
sgroupBy :: Int -> Float -> [(Int, Int, String, Float)] -> [(Int, Float)]
printGroupBy :: [(Int, Float)] -> IO()

salesDB [] = []

salesDB (x:xs) = do
    let items = split x '|'
    (read (items!!0)::Int, read (items!!1)::Int, items!!2, read (items!!3)::Float):salesDB xs
    
salesSortf a b
    | s_cust a < s_cust b = LT
    | otherwise = GT

orderSales [] = []

orderSales a = do
    sortBy salesSortf a
    
sgroupBy i c [] = do
    if c==0 then []
    else
        (i, c):[]

sgroupBy i c (x:xs) = do
    if ((s_cust x)==i) then
        sgroupBy i (c+(s_total x)) xs
    else
        (i, c):sgroupBy (s_cust x) 0 (x:xs)

printGroupBy [] = do
    return ()

printGroupBy (x:xs) = do
    let line = printf "%d\t%f" (fst x) (snd x)
    putStrLn line
    printGroupBy xs

main = do
    sales <- readFile "SalesDB.apd"
    let saleslines = lines sales
    let sDB = orderSales (salesDB saleslines)
    let r = sgroupBy (s_cust (sDB!!0)) 0 sDB
    let title = "Cust\tSales"
    putStrLn title
    printGroupBy r