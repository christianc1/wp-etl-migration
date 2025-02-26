<?php
/**
 * Factory class for creating ETL adapters
 *
 * @package TenupETL\Classes\Load\Factories
 */

namespace TenupETL\Classes\Load\Factories;

use TenupETL\Classes\Config\GlobalConfig;
use TenupETL\Classes\Load\Loaders;
use TenupETL\Classes\Transform\Transformers\SelectPrefixTransformer;
use Flow\ETL\Loader\TransformerLoader;
use Flow\ETL\Loader;
use Flow\ETL\Adapter\WordPress\{WPPostsLoader, WPPostMetaLoader, WPTermsLoader, WPPostTermsLoader};
use function Flow\ETL\Adapter\Json\{to_json};
use function Flow\ETL\Adapter\CSV\{to_csv};


/**
 * Creates adapter instances for different file types
 */
class AdapterFactory {

	/**
	 * Constructor
	 *
	 * @param GlobalConfig $config Global configuration instance
	 */
	public function __construct( protected readonly GlobalConfig $config ) {}

	/**
	 * Create adapter instance based on load operation configuration
	 *
	 * @param array  $load_operation Full load operation configuration
	 * @return mixed Adapter instance
	 */
	public function create( array $load_operation ): Loader {
		$loader = $load_operation['loader'];

		return match ( strtolower( $loader ) ) {
			'json' => $this->create_json_loader( $load_operation ),
			'csv' => $this->create_csv_loader( $load_operation ),
			'wp_post' => new Loaders\WordPressPostLoader( $load_operation, $this->config ),
			'wp_post_meta' => new WPPostMetaLoader( [] ),
			'wp_post_terms' => new WPPostTermsLoader( [] ),
			'wp_post_media' => new Loaders\WordPressMediaLoader( $load_operation, $this->config ),
			'wp_term' => new WPTermsLoader( [] ),
			'ledger' => new Loaders\LedgerLoader( $load_operation, $this->config ),
			'custom' => new($load_operation['pipeline'])( $load_operation, $this->config ),
			default => $this->create_json_loader( $load_operation ),
		};
	}

	public function create_csv_loader( $load_operation ) {
		$loader = to_csv( $this->determine_destination( $load_operation ) );

		$prefix = $load_operation['prefix'] ?? [];
		if ( ! empty( $prefix ) ) {
			return $this->with_prefix_pretransformer( $loader, $prefix );
		}

		return $loader;
	}

	public function create_json_loader( $load_operation ) {
		$flags = JSON_INVALID_UTF8_SUBSTITUTE;
		if ( $load_operation['options']['flags'] ) {
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
					? $filename . $extension
					: $filename . '-' . time() . '.' . $extension
				)
			]
		);

		return $destination;
	}

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
