<?php


$dbconn = pg_connect("host=localhost dbname=test") or die("could not connect".pg_last_error());

$result = pg_query("DELETE FROM kt_test") or die('Query failed: ' . pg_last_error());

$before_insert = time();

$result = pg_prepare($dbconn, "insert_q", "INSERT INTO kt_test VALUES ($1, $2)");

for($i = 0; $i < 1000000; $i++) {
  $result = pg_execute($dbconn, "insert_q",array("k".$i, "v".$i));
}

print_r("Insert took ".(time()-$before_insert));

$before_op = time();
$query = "select count(*) from kt_test";
$result = pg_query($query) or die('Query failed: ' . pg_last_error());
print_r("count took".(time()-$before_op));
while ($line = pg_fetch_array($result, null, PGSQL_ASSOC)) {
  print_r($line);
}

$before_op = time();
$query = "select * from kt_test where key='k1'";
$result = pg_query($query) or die('Query failed: ' . pg_last_error());
print_r("select one took".(time()-$before_op));
while ($line = pg_fetch_array($result, null, PGSQL_ASSOC)) {
  print_r($line);
}

$before_op = time();
$query = "select * from kt_test";
$result = pg_query($query) or die('Query failed: ' . pg_last_error());

print_r("Done!");
?>
