<?php
/**
 * DataNormalizer class file
 *
 * This file contains the DataNormalizer class which provides functionality
 * for normalizing data frames by standardizing column names and adding prefixes.
 *
 * @package TenupETL\Classes\Extract\Normalizers
 */

namespace TenupETL\Classes\Extract\Normalizers;

use function TenupETL\Classes\Transform\Transformers\{prefix_ref, rename_regex};

/**
 * Class DataNormalizer
 *
 * Handles normalization of data frames including column name standardization
 * and prefix addition.
 *
 * @package TenupETL\Classes\Extract\Normalizers
 */
class DataNormalizer {
	/**
	 * Normalize data frame
	 *
	 * Standardizes column names to snake case and removes invalid characters.
	 *
	 * @param mixed $data_frame Data frame to normalize.
	 * @return mixed Normalized data frame.
	 */
	public function normalize( $data_frame ) {
		return $data_frame
			->renameAllStyle( 'snake' )
			->transform(
				rename_regex( '/[\x00-\x1F\x80-\xFF]/', '' )
			);
	}

	/**
	 * Add prefix to data frame columns
	 *
	 * Adds a specified prefix to all column names in the data frame.
	 *
	 * @param mixed  $data_frame Data frame to prefix.
	 * @param string $prefix     Prefix to add.
	 * @return mixed Prefixed data frame.
	 */
	public function prefix( $data_frame, string $prefix = '' ) {
		return $data_frame->transform(
			prefix_ref( $prefix )
		);
	}
}
