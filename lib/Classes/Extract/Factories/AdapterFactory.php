<?php
/**
 * Factory class for creating ETL adapters
 *
 * @package TenupETL\Classes\Extract\Factories
 */

namespace TenupETL\Classes\Extract\Factories;

use TenupETL\Classes\Extract\Extractors\LocalFileSystemExtractor;
use TenupETL\Utils\WithLogging;
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\XML\from_xml;
use function Flow\ETL\Adapter\WordPress\{from_wp_posts, from_wp_terms, from_wp_users};

/**
 * Creates adapter instances for different file types
 */
class AdapterFactory {
	use WithLogging;

	/**
	 * Constructor
	 *
	 * @param LocalFileSystemExtractor $fs_extractor File system extractor instance
	 */
	public function __construct(
		protected LocalFileSystemExtractor $fs_extractor
	) {}

	/**
	 * Create adapter instance based on extraction configuration
	 *
	 * @param string $type       Adapter type
	 * @param array  $extraction Full extraction configuration
	 * @return mixed Adapter instance
	 */
	public function create( string $type, array $extraction ): mixed {
		if ( ! $type ) {
			$this->log( 'No adapter specified, defaulting to CSV', 'warning' );
			$type = 'csv';
		}

		return match ( $type ) {
			'csv' => $this->create_csv_adapter( $extraction ),
			'json' => $this->create_json_adapter( $extraction ),
			'xml' => $this->create_xml_adapter( $extraction ),
			'rss' => $this->create_rss_adapter( $extraction ),
			'wp_posts' => $this->create_wp_posts_adapter( $extraction ),
			'wp_terms' => $this->create_wp_terms_adapter( $extraction ),
			'wp_users' => $this->create_wp_users_adapter( $extraction ),
			default => $this->create_csv_adapter( $extraction ),
		};
	}

	/**
	 * Create CSV adapter instance
	 *
	 * @param array $extraction Extraction configuration
	 * @return mixed CSV adapter instance
	 */
	private function create_csv_adapter( array $extraction ): mixed {
		$file = $this->fs_extractor->extract( $extraction['source']['localfs'] );

		if ( ! $file ) {
			$this->log( 'No CSV file found in extraction configuration', 'error' );
			return null;
		}

		return from_csv( $file->getRealPath() );
	}

	/**
	 * Create JSON adapter instance
	 *
	 * @param array $extraction Extraction configuration
	 * @return mixed JSON adapter instance
	 */
	private function create_json_adapter( array $extraction ): mixed {
		$file = $this->fs_extractor->extract( $extraction['source']['localfs'] );

		if ( ! $file ) {
			$this->log( 'No JSON file found in extraction configuration', 'error' );
			return null;
		}

		return from_json( $file->getRealPath() );
	}

	/**
	 * Create RSS adapter instance
	 *
	 * @param array $extraction Extraction configuration
	 * @return mixed RSS adapter instance
	 */
	private function create_rss_adapter( array $extraction ): mixed {
		$url = $extraction['source']['rss']['url'];
		$this->log( "Fetching XML from URL: {$url}", 'debug' );

		$adapter = null;

		// Create output directory if it doesn't exist
		$output_dir = TENUP_ETL_PLUGIN_DIR . 'output/.cache/rss';
		if ( ! file_exists( $output_dir ) ) {
			mkdir( $output_dir, 0755, true );
		}

		// Create a cacheable temp file.
		$filename    = md5( $url ) . '.xml';
		$output_path = $output_dir . '/' . $filename;

		// Check if the file exists
		if ( file_exists( $output_dir . '/' . $filename ) ) {
			$this->log( "Using cached file: {$output_dir}/{$filename}", 'debug' );
			$adapter = from_xml( $output_dir . '/' . $filename );
		} else {
			$response = wp_remote_get( $url, [ 'timeout' => 60 ] );

			if ( is_wp_error( $response ) ) {
				$this->log( "Failed to fetch XML: {$response->get_error_message()}", 'error' );
				return null;
			}

			$content = wp_remote_retrieve_body( $response );
			if ( empty( $content ) ) {
				$this->log( 'Empty response from XML source', 'debug' );
				return null;
			}

			// Write content to file
			$result = file_put_contents( $output_path, $content );
			if ( $result === false ) {
				$this->log( 'Failed to write RSS content to file', 'debug' );
				return null;
			}

			$this->log( "RSS content written to: {$output_path}", 'debug' );

			$adapter = from_xml( $output_path );
		}

		return $adapter->withXMLNodePath( $extraction['args']['xmlNodePath'] ?? 'rss/channel/item' );
	}

	/**
	 * Create XML adapter instance
	 *
	 * @param array $extraction Extraction configuration
	 * @return mixed XML adapter instance
	 */
	private function create_xml_adapter( array $extraction ): mixed {
		$file = $this->fs_extractor->extract( $extraction['source']['localfs'] );
		if ( ! $file ) {
			$this->log( 'No XML file found in extraction configuration', 'error' );
			return null;
		}
		$adapter = from_xml( $file->getRealPath() );

		// Configure XML node path if specified
		if ( ! empty( $extraction['args']['xmlNodePath'] ) ) {
			$adapter = $adapter->withXMLNodePath( 'rss/channel/item' );
		}

		return $adapter;
	}

	/**
	 * Create WordPress Posts adapter instance
	 *
	 * @param array $extraction Extraction configuration
	 * @return mixed WordPress Posts adapter instance
	 */
	private function create_wp_posts_adapter( array $extraction ): mixed {
		$adapter = from_wp_posts(
			[
				'defaults' => $extraction['args']['query'] ?? [],
			]
		);

		// Apply feature flags
		if ( ! empty( $extraction['args']['withExpandedAuthorData'] ) ) {
			$adapter = $adapter->withExpandedAuthorData();
		}

		if ( isset( $extraction['args']['withTaxonomies'] ) ) {
			$adapter = $adapter->withTaxonomies( $extraction['args']['withTaxonomies'] );
		}

		if ( isset( $extraction['args']['withMeta'] ) ) {
			$adapter = $adapter->withMeta( $extraction['args']['withMeta'] );
		}

		return $adapter;
	}

	/**
	 * Create WordPress Terms adapter instance
	 *
	 * @param array $extraction Extraction configuration
	 * @return mixed WordPress Terms adapter instance
	 */
	private function create_wp_terms_adapter( array $extraction ): mixed {
		$adapter = from_wp_terms(
			[
				'defaults' => $extraction['args']['query'] ?? [],
			]
		);

		// Apply feature flags
		if ( isset( $extraction['args']['withMeta'] ) ) {
			$adapter = $adapter->withMeta( $extraction['args']['withMeta'] );
		}

		return $adapter;
	}

	/**
	 * Create WordPress Users adapter instance
	 *
	 * @param array $extraction Extraction configuration
	 * @return mixed WordPress Users adapter instance
	 */
	private function create_wp_users_adapter( array $extraction ): mixed {
		$adapter = from_wp_users(
			[
				'defaults' => $extraction['args']['query'] ?? [],
			]
		);

		// Apply feature flags
		if ( isset( $extraction['args']['withMeta'] ) ) {
			$adapter = $adapter->withMeta( $extraction['args']['withMeta'] );
		}

		if ( isset( $extraction['args']['withCapabilities'] ) ) {
			$adapter = $adapter->withCapabilities( $extraction['args']['withCapabilities'] );
		}

		return $adapter;
	}
}
