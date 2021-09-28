
$file_from = 'C:\ag-ship-data\RETAIL_AGIN_DATA.txt'
# $file_from = 'C:\temp\testfile.txt'
$file_to = 'C:\temp\RETAIL_AGIN_DATA_UTF8.txt'
# $file_to = 'C:\temp\test.txt'
$log_file = 'C:\temp\ibp-stress-test-dataload\log.log'

$target_custprod_ct = 60000000
$target_product_ct = 17000
$target_store_ct = 40000

# (measure-command{[System.IO.File]::ReadLines($file_from)}).TotalSeconds

function log($txt){
	Add-Content -Path $log_file -Value $txt
}

function convert-encoding ($NumLinesToSkip) {

	# [System.IO.File]::ReadLines($file_from) | Select -Skip 1000000 | Select -first 1000000 | Set-Content -Encoding utf8 $file_to

	# [system.io.file]::WriteAllText($file_to, ([System.IO.File]::ReadAllText($file_from)),[text.encoding]::utf8) # fails - not enough memory; otherwise good idea.

	New-Item -Path $log_file -ItemType File -Force >$nul

	log((get-date).ToString())

	$elapsed_seconds = (measure-command {
		$sw = new-object system.IO.StreamWriter($file_to, [System.Text.Encoding]::UTF8)

		$ct = 0
		
		[System.IO.File]::ReadLines($file_from) |  Select -Skip $NumLinesToSkip | %{
			
			$sw.write($_)
			$sw.write("`r`n")
			$ct+=1
			if($ct % 1000000 -eq 0) {log("Converted $ct rows.")}
		}
	}).TotalSeconds

	log("Completed file conversion in $elapsed_seconds seconds.")
	log((get-date).ToString())
	 
	$sw.close()
}

function write-without-header {
	New-Item -Path $log_file -ItemType File -Force >$nul

	log((get-date).ToString())

	$elapsed_seconds = (measure-command {
		$sw = new-object system.IO.StreamWriter($file_to, [System.Text.Encoding]::UTF8)

		$ct = 0

		[System.IO.File]::ReadLines($file_from) | Select -Skip 1 | %{
			
			$sw.writeline($_)
			$ct+=1
			if($ct % 1000000 -eq 0) {log("$ct rows written to $file_to.")}
		}
	}).TotalSeconds

	log("Completed file write in $elapsed_seconds seconds.")
	log((get-date).ToString())
	 
	$sw.close()
}

function find-badvalues {
	[System.IO.File]::ReadLines($file_from) | %{
		
		Select-String '*OCCN_DESC*'
	}
}

function run-mysql($param_script){
	$param_script | mysql -u root --password='mysql' | Add-Content -Path $log_file 
}

function export-sql-reports {

log((get-date).ToString())

log('SUMMARY LEVEL COUNTS:')
log('')

<# run-mysql(@"
	select 'Distinct AGIN_NBR:' as '';
	SELECT COUNT(DISTINCT AGIN_NBR) 
	FROM jim.AG_SHIP_WHSL ;
"@)
 #>
log('Distinct RTLR_CHAIN_NBR,RTLR_STR_NBR:')
run-mysql(@"
	SELECT COUNT(DISTINCT RTLR_CHAIN_NBR,RTLR_STR_NBR) 
	FROM jim.AG_SHIP_WHSL ;
"@)
<# run-mysql(@"
	select 'Distinct RTLR_CHAIN_NBR:' as '';
	SELECT COUNT(DISTINCT RTLR_CHAIN_NBR) 
	FROM jim.AG_SHIP_WHSL ;
"@)
run-mysql(@"
	select 'Distinct RTLR_STR_NBR:' as '';
	SELECT COUNT(DISTINCT RTLR_STR_NBR) 
	FROM jim.AG_SHIP_WHSL ;
"@)
 #>
<# run-mysql(@"
	select 'Distinct RTLR_CHAIN_NBR,AGIN_NBR:' as '';
	SELECT COUNT(DISTINCT RTLR_CHAIN_NBR,AGIN_NBR) 
	FROM jim.AG_SHIP_WHSL ;
"@)
 #>
log('Distinct RTLR_CHAIN_NBR,RTLR_STR_NBR,AGIN_NBR:')
run-mysql(@"
	SELECT COUNT(DISTINCT RTLR_CHAIN_NBR,RTLR_STR_NBR,AGIN_NBR) 
	FROM jim.AG_SHIP_WHSL ;
"@)

<# run-mysql(@"
	select 'Distinct FILL_DC,AGIN_NBR:' as '';
	SELECT COUNT(DISTINCT FILL_DC,AGIN_NBR) 
	FROM jim.AG_SHIP_WHSL ;
"@)
 #>
log('SUMMARY ED_NON_ED LEVEL COUNTS:')
log('')


<# run-mysql(@"
	select 'Distinct AGIN_NBR by ED_NON_ED:' as '';
	SELECT CASE WHEN OCCN_DESC='EVERYDAY' THEN 'ED' ELSE 'NON-ED' END as ED_NON_ED
		, COUNT(DISTINCT AGIN_NBR) 
	FROM jim.AG_SHIP_WHSL 
	GROUP BY CASE WHEN OCCN_DESC='EVERYDAY' THEN 'ED' ELSE 'NON-ED' END;
"@)
 #>
log('Distinct RTLR_CHAIN_NBR,RTLR_STR_NBR by ED_NON_ED:')
run-mysql(@"
	SELECT CASE WHEN OCCN_DESC='EVERYDAY' THEN 'ED' ELSE 'NON-ED' END as ED_NON_ED
		, COUNT(DISTINCT RTLR_CHAIN_NBR,RTLR_STR_NBR) 
	FROM jim.AG_SHIP_WHSL 
	GROUP BY CASE WHEN OCCN_DESC='EVERYDAY' THEN 'ED' ELSE 'NON-ED' END;
"@)

<# run-mysql(@"
	select 'Distinct RTLR_CHAIN_NBR,AGIN_NBR by ED_NON_ED:' as '';
	SELECT CASE WHEN OCCN_DESC='EVERYDAY' THEN 'ED' ELSE 'NON-ED' END as ED_NON_ED
		, COUNT(DISTINCT RTLR_CHAIN_NBR,AGIN_NBR) 
	FROM jim.AG_SHIP_WHSL 
	GROUP BY CASE WHEN OCCN_DESC='EVERYDAY' THEN 'ED' ELSE 'NON-ED' END;
"@)
 #>
log('Distinct RTLR_CHAIN_NBR,RTLR_STR_NBR,AGIN_NBR by ED_NON_ED:')
run-mysql(@"
	SELECT CASE WHEN OCCN_DESC='EVERYDAY' THEN 'ED' ELSE 'NON-ED' END as ED_NON_ED
		, COUNT(DISTINCT RTLR_CHAIN_NBR,RTLR_STR_NBR,AGIN_NBR) 
	FROM jim.AG_SHIP_WHSL 
	GROUP BY CASE WHEN OCCN_DESC='EVERYDAY' THEN 'ED' ELSE 'NON-ED' END;
"@)

<# run-mysql(@"
	select 'Distinct FILL_DC,AGIN_NBR by ED_NON_ED:' as '';
	SELECT CASE WHEN OCCN_DESC='EVERYDAY' THEN 'ED' ELSE 'NON-ED' END as ED_NON_ED
		, COUNT(DISTINCT FILL_DC,AGIN_NBR) 
	FROM jim.AG_SHIP_WHSL 
	GROUP BY CASE WHEN OCCN_DESC='EVERYDAY' THEN 'ED' ELSE 'NON-ED' END;
"@)
 #>
log('SUMMARY OCCN_DESC LEVEL COUNTS:')
log('')


<# run-mysql(@"
	select 'Distinct AGIN_NBR by OCCN_DESC:' as '';
	SELECT OCCN_DESC, COUNT(DISTINCT AGIN_NBR) FROM jim.AG_SHIP_WHSL GROUP BY OCCN_DESC;
"@)

run-mysql(@"
	select 'Distinct RTLR_CHAIN_NBR by OCCN_DESC:' as '';
	SELECT OCCN_DESC, COUNT(DISTINCT RTLR_CHAIN_NBR) FROM jim.AG_SHIP_WHSL GROUP BY OCCN_DESC;
"@)

run-mysql(@"
	select 'Distinct RTLR_CHAIN_NBR,AGIN_NBR by OCCN_DESC:' as '';
	SELECT OCCN_DESC, COUNT(DISTINCT RTLR_CHAIN_NBR,AGIN_NBR) FROM jim.AG_SHIP_WHSL GROUP BY OCCN_DESC;
"@)

run-mysql(@"
	select 'Distinct RTLR_STR_NBR,AGIN_NBR by OCCN_DESC:' as '';
	SELECT OCCN_DESC, COUNT(DISTINCT RTLR_STR_NBR,AGIN_NBR) FROM jim.AG_SHIP_WHSL GROUP BY OCCN_DESC;
"@)

run-mysql(@"
	select 'Distinct FILL_DC,AGIN_NBR by OCCN_DESC:' as '';
	SELECT OCCN_DESC, COUNT(DISTINCT FILL_DC,AGIN_NBR) FROM jim.AG_SHIP_WHSL GROUP BY OCCN_DESC;
"@)
 #>
log('')
log('End of Script.')
log((get-date).ToString())

}

function Create-Product {

	try {
		# New-Item -Path $log_file -ItemType File -Force >$nul

		log((get-date).ToString())

		$file = 'c:\temp\ibp-stress-test-dataload\product.txt'
		
		If( Test-Path -Path $file ) { Remove-Item $file }

		$elapsed_seconds = (measure-command {
			$sw = new-object system.IO.StreamWriter($file, [System.Text.Encoding]::UTF8)

			$ct = 0

			for ($i=1; $i -le $target_product_ct; $i++) { 
				$sw.writeline( '1' + $i.ToString().PadLeft(5,'0') ) 
				$ct+=1
				if($ct % 1000000 -eq 0) {log("$ct rows written to $file_to.")}
			}
		}).TotalSeconds

		log("Completed file write in $elapsed_seconds seconds.")
		log((get-date).ToString())
	}
	finally {
		$sw.close()
	}
}

function Create-Store {

	try {
		# New-Item -Path $log_file -ItemType File -Force >$nul

		log((get-date).ToString())

		$file = 'c:\temp\ibp-stress-test-dataload\store.txt'
		
		If( Test-Path -Path $file ) { Remove-Item $file }

		$elapsed_seconds = (measure-command {
			$sw = new-object system.IO.StreamWriter($file, [System.Text.Encoding]::UTF8)

			$ct = 0

			for ($i=1; $i -le $target_store_ct; $i++) { 
				$sw.writeline( 'STR' + $i.ToString().PadLeft(5,'0') ) 
				$ct+=1
				if($ct % 1000000 -eq 0) {log("$ct rows written to $file_to.")}
			}
		}).TotalSeconds

		log("Completed file write in $elapsed_seconds seconds.")
		log((get-date).ToString())
	}
	finally	{
		$sw.close()
	}
}

function Create-LocationSource {
	try {
		$ProductArray = gc 'c:\temp\ibp-stress-test-dataload\product.txt'
		$StoreArray = gc 'c:\temp\ibp-stress-test-dataload\store.txt'
		$NumProductsPerStore = $target_custprod_ct / $StoreArray.GetUpperBound(0)
		$ProductIndex = 0
		$ct = 0
		log((get-date).ToString())
		$file = 'c:\temp\ibp-stress-test-dataload\location-source.txt'
		If( Test-Path -Path $file ) { Remove-Item $file }
		$elapsed_seconds = (measure-command {
			$sw = new-object system.IO.StreamWriter($file, [System.Text.Encoding]::UTF8)
			$sw.writeline(  @('Product','Location To','Location From','Min Lot Size','Incremental Lot Size','ROP','Inventory') -join ','  )
			for ($i=0; $i -le $StoreArray.GetUpperBound(0); $i++) {
			# for ($i=0; $i -le 10; $i++) {
				for ($j=0; $j -lt $NumProductsPerStore; $j++) {
					if($ProductIndex -gt $ProductArray.GetUpperBound(0)) {$ProductIndex=0}
					$sw.writeline(  @( $ProductArray[$ProductIndex],$StoreArray[$i],'DC1','3','3','3','2' ) -join ','  )
					$ProductIndex++
					$ct++
					if($ct % 1000000 -eq 0) {log("$ct rows written to $file_to.")}
				}
			}
		}).TotalSeconds
		log("Completed file write in $elapsed_seconds seconds.")
		log((get-date).ToString())
	}
	finally {$sw.Close()}
}

function Split-File ($param_src_file) {
	# New-Item -Path $log_file -ItemType File -Force >$nul

	log((get-date).ToString())

	$rows_per_file = 2000000

	$i = 1
	
    # target file
	$ext = ($param_src_file -Split '\.')[1]
	$param_src_file_main = ($param_src_file -Split '\.')[0]
	$tgt_file = $param_src_file_main + '-' + $i.ToString() + '.' + $ext
 	
 	$sw = new-object system.IO.StreamWriter($tgt_file, [System.Text.Encoding]::UTF8)

	$ct = 0

	$hdr = ([System.IO.File]::ReadLines($param_src_file) | Select -First 1)
	
	# Split the File.
	[System.IO.File]::ReadLines($param_src_file) | Select -Skip 1 | %{
		$sw.writeline($_)
		$ct+=1
		if($ct -gt $rows_per_file) {
			$ct = 0
			$sw.Close()
			$i++
			$tgt_file = $param_src_file_main + '-' + $i.ToString() + '.' + $ext
			$sw = new-object system.IO.StreamWriter($tgt_file, [System.Text.Encoding]::UTF8)
		}
	}	
	

	log("Completed file write in $elapsed_seconds seconds.")
	log((get-date).ToString())
	 
	$sw.Close()
	
}

function Compress-File {	
	compress-archive -path .\location-source.txt -destinationpath .\location-source.zip -compressionlevel optimal     
}

# Create-Product
# Create-Store
# Create-LocationSource
Split-File('c:\temp\ibp-stress-test-dataload\location-source.txt')
