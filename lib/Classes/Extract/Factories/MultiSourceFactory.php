<?php
/**
 * Factory for creating multi-source iterators
 *
 * @package TenupETL\Classes\Extract\Factories
 */

namespace TenupETL\Classes\Extract\Factories;

use TenupETL\Classes\Extract\Iterators\RSSMultiSourceIterator;
use TenupETL\Utils\WithLogging;

/**
 * Creates source iterators based on extraction configuration
 */
class MultiSourceFactory {
	use WithLogging;

	/**
	 * Create a source iterator based on extraction configuration
	 *
	 * @param array $extraction Extraction configuration
	 * @return \Iterator|null Iterator for source URLs
	 */
	public function create( array $extraction ): ?\Iterator {
		if ( empty( $extraction['source'] ) ) {
			$this->log( 'No source configuration provided', 'error' );
			return null;
		}

		// Determine source type from first key in source config
		$adapter = $extraction['adapter'];

		return match ( $adapter ) {
			'rss' => $this->create_rss_iterator( $extraction['source']['rss'] ),
			default => null,
		};
	}

	/**
	 * Create RSS source iterator
	 *
	 * @param array $config RSS source configuration
	 * @return \Iterator|null Iterator for RSS URLs
	 */
	protected function create_rss_iterator( array $config ): ?\Iterator {
		if ( empty( $config['url'] ) ) {
			$this->log( 'No URL provided in RSS configuration', 'error' );
			return null;
		}

		return new RSSMultiSourceIterator( $config );
	}
}
