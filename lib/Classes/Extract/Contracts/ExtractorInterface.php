<?php
/**
 * ExtractorInterface interface file
 *
 * This file contains the ExtractorInterface interface which defines the contract
 * for data extraction functionality. Classes implementing this interface are responsible
 * for extracting data from various sources like local filesystem, remote APIs, etc.
 *
 * @package TenupETL\Classes\Extract\Contracts
 */

namespace TenupETL\Classes\Extract\Contracts;

/**
 * Interface ExtractorInterface
 *
 * Defines the contract for data extraction functionality.
 * Implementing classes must provide the extract() method to handle
 * data extraction from their specific source.
 *
 * @package TenupETL\Classes\Extract\Contracts
 */
interface ExtractorInterface {
	/**
	 * Extract data from source
	 *
	 * Extracts data based on the provided configuration array. The exact format
	 * of the configuration will depend on the specific extractor implementation.
	 *
	 * @param array $config Extraction configuration containing source details and options.
	 * @return mixed Extracted data in implementation-specific format.
	 */
	public function extract( array $config );
}
