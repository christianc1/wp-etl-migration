<?php
/**
 * LocalFileSystemExtractor class file
 *
 * This file contains the LocalFileSystemExtractor class which provides functionality
 * for extracting files from the local filesystem using Symfony's Finder component.
 *
 * @package TenupETL\Classes\Extract\Extractors
 */

namespace TenupETL\Classes\Extract\Extractors;

use Symfony\Component\Finder\Finder;
use TenupETL\Classes\Extract\Contracts\ExtractorInterface;
use TenupETL\Utils\WithLogging;
use SplFileInfo;

/**
 * Class LocalFileSystemExtractor
 *
 * Handles extraction of files from the local filesystem based on provided configuration.
 * Uses Symfony's Finder component for file operations.
 *
 * @package TenupETL\Classes\Extract\Extractors
 */
class LocalFileSystemExtractor implements ExtractorInterface {
	use WithLogging;

	/**
	 * Constructor
	 *
	 * @param string $base_path Base path for file operations.
	 */
	public function __construct( protected string $base_path ) {
		$this->base_path = rtrim( ltrim( $base_path, './' ), '/' );
	}

	/**
	 * Extract a single file from local filesystem
	 *
	 * @param array $config Extraction configuration containing file path.
	 * @return SplFileInfo|null Found file or null if not found.
	 */
	public function extract( array $config ): ?SplFileInfo {
		if ( empty( $config['file'] ) ) {
			$this->log( 'No file specified in configuration', 'error' );
			return null;
		}

		$file_path = $config['file'];
		$full_path = TENUP_ETL_PLUGIN_DIR . '/' . $this->base_path;

		$this->log( "Searching for file {$file_path} in {$full_path}", 'debug' );

		// Handle file paths with directories
		if ( strpos( $file_path, '/' ) !== false ) {
			// Split the path into directory and filename
			$path_parts = explode( '/', ltrim( $file_path, '/' ) );
			$filename = array_pop( $path_parts );
			$subdirectory = implode( '/', $path_parts );

			// Update the full path to include the subdirectory
			$full_path = $full_path . '/' . $subdirectory;

			$this->log( "Looking for file {$filename} in subdirectory {$subdirectory}", 'debug' );

			$finder = new Finder();
			$finder
				->files()
				->name( $filename )
				->in( $full_path );
		} else {
			// Original behavior for simple filenames
			$finder = new Finder();
			$finder
				->files()
				->name( $file_path )
				->in( $full_path );
		}

		if ( ! $finder->hasResults() ) {
			$this->log( "File not found: {$file_path} in {$full_path}", 'error' );
			return null;
		}

		foreach ( $finder as $file ) {
			return $file; // Return the first file found
		}

		return null; // Should never reach here due to hasResults() check
	}
}
