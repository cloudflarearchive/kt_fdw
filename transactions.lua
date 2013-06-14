kt = __kyototycoon__
db = kt.db

function ktfdwbegintransaction(inmap, outmap)
   if not db:begin_transaction() then
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

function ktfdwcommit(inmap, outmap)
   if not db:end_transaction(true) then
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

function ktfdwabort(inmap, outmap)
   if not db:end_transaction(false) then
      return kt.RVEINTERNAL
   end
   return kt.RVSUCCESS
end

-- list all records
function list(inmap, outmap)
   local cur = db:cursor()
   cur:jump()
   while true do
      local key, value, xt = cur:get(true)
      if not key then break end
      outmap[key] = value
   end
   return kt.RVSUCCESS
end
