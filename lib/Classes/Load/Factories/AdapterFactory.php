<?php

declare(strict_types=1);

/**
 * Factory class for creating ETL adapters
 *
 * This class handles creation of various loader adapters for ETL operations.
 * It supports JSON, CSV, WordPress posts/terms/users and custom loaders.
 * Each loader can optionally have prefix transformers applied.
 *
 * @package TenupETL\Classes\Load\Factories
 */

namespace TenupETL\Classes\Load\Factories;

use TenupETL\Classes\Config\GlobalConfig;
use TenupETL\Classes\Load\Loaders;
use TenupETL\Classes\Transform\Transformers\SelectPrefixTransformer;
use Flow\ETL\Loader\TransformerLoader;
use Flow\ETL\Loader;
use Flow\ETL\Adapter\WordPress\Loaders\{
	WPPostsLoader,
	WPPostMetaLoader,
	WPTermsLoader,
	WPPostTermsLoader,
	WPUserLoader
};

use function Flow\ETL\Adapter\Json\{to_json_lines, to_json};
use function Flow\ETL\Adapter\CSV\{to_csv};
use function Flow\ETL\Adapter\WordPress\{
	to_wp_post_meta,
	to_wp_post_terms,
	to_wp_user,
	to_wp_terms
};

/**
 * Creates adapter instances for different file types
 */
class AdapterFactory {

	/**
	 * Constructor
	 *
	 * @param GlobalConfig $config Global configuration instance used across loaders
	 */
	public function __construct( protected readonly GlobalConfig $config ) {}

	/**
	 * Create adapter instance based on load operation configuration
	 *
	 * Creates and returns the appropriate loader based on the specified type.
	 * Supports JSON, CSV, WordPress entities, and custom loaders.
	 *
	 * @param array<string,mixed> $load_operation Full load operation configuration array
	 * @return Loader The configured loader instance
	 */
	public function create( array $load_operation ): Loader {
		$loader = $load_operation['loader'];

		return match ( strtolower( $loader ) ) {
			'json' => $this->create_json_loader( $load_operation ),
			'csv' => $this->create_csv_loader( $load_operation ),
			'wp_post' => new Loaders\WordPressPostLoader( $load_operation, $this->config ),
			'wp_post_meta' => to_wp_post_meta( [] ),
			'wp_post_terms' => to_wp_post_terms( [] ),
			'wp_post_media' => new Loaders\WordPressMediaLoader( $load_operation, $this->config ),
			'wp_term' => to_wp_terms( [] ),
			'wp_user' => to_wp_user( [] ),
			'ledger' => new Loaders\LedgerLoader( $load_operation, $this->config ),
			'custom' => new($load_operation['pipeline'])( $load_operation, $this->config ),
			default => $this->create_json_loader( $load_operation ),
		};
	}

	/**
	 * Creates a CSV loader with optional prefix transformer
	 *
	 * @param array<string,mixed> $load_operation Load operation configuration
	 * @return Loader Configured CSV loader
	 */
	public function create_csv_loader( array $load_operation ): Loader {
		$loader = to_csv( $this->determine_destination( $load_operation ) );

		$prefix = $load_operation['prefix'] ?? [];
		if ( ! empty( $prefix ) ) {
			return $this->with_prefix_pretransformer( $loader, $prefix );
		}

		return $loader;
	}

	/**
	 * Creates a JSON loader with optional prefix transformer
	 *
	 * @param array<string,mixed> $load_operation Load operation configuration
	 * @return Loader Configured JSON loader
	 */
	public function create_json_loader( array $load_operation ): Loader {
		$flags = JSON_INVALID_UTF8_SUBSTITUTE;
		if ( isset( $load_operation['options']['flags'] ) && is_array( $load_operation['options']['flags'] ) ) {
			foreach ( $load_operation['options']['flags'] as $flag ) {
				$flags |= constant( $flag );
			}
		}

		$loader = to_json( $this->determine_destination( $load_operation ), $flags );

		$prefix = $load_operation['prefix'] ?? [];
		if ( ! empty( $prefix ) ) {
			return $this->with_prefix_pretransformer( $loader, $prefix );
		}

		return $loader;
	}

	/**
	 * Determines the destination file path for file-based loaders
	 *
	 * Constructs the full file path based on the destination config.
	 * Handles overwriting vs timestamped files.
	 *
	 * @param array<string,mixed> $load_operation Load operation configuration
	 * @return string Full destination file path
	 */
	public function determine_destination( array $load_operation ): string {
		$extension = pathinfo( $load_operation['destination']['file'], PATHINFO_EXTENSION );
		$overwrite = $load_operation['overwrite'] ?? false;
		$filename  = basename( $load_operation['destination']['file'], '.' . $extension );

		$destination = join(
			DIRECTORY_SEPARATOR,
			[
				untrailingslashit( TENUP_ETL_PLUGIN_DIR ),
				untrailingslashit( ltrim( $load_operation['destination']['path'], './' ) ),
				( $overwrite
					? $filename . '.' . $extension
					: $filename . '-' . time() . '.' . $extension
				)
			]
		);

		return $destination;
	}

	/**
	 * Wraps a loader with a prefix transformer
	 *
	 * Allows filtering entries by prefix before loading.
	 *
	 * @param Loader             $loader The loader to wrap
	 * @param string|array<int,string> $prefix Prefix(es) to filter by
	 * @return Loader Wrapped loader with prefix transformer
	 */
	public function with_prefix_pretransformer( Loader $loader, string | array $prefix ): Loader {
		if ( empty( $prefix ) ) {
			return $loader;
		}

		if ( ! is_array( $prefix ) ) {
			$prefix = [ $prefix ];
		}

		return new TransformerLoader(
			new SelectPrefixTransformer( $prefix, count( $prefix ) === 1 ),
			$loader,
		);
	}
}
