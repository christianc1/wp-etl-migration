<?php
/**
 * Iterator for RSS multi-source URLs
 *
 * @package TenupETL\Classes\Extract\Iterators
 */

namespace TenupETL\Classes\Extract\Iterators;

use TenupETL\Utils\WithLogging;

/**
 * Iterates through RSS URLs based on pagination configuration
 */
class RSSMultiSourceIterator implements \Iterator {
	use WithLogging;

	/**
	 * Current position in iteration
	 *
	 * @var int
	 */
	private int $position = 0;

	/**
	 * Constructor
	 *
	 * @param array $config RSS configuration
	 */
	public function __construct( protected array $config ) {
		if ( ! empty( $this->config['pagination'] ) ) {
			$this->position = $this->config['pagination']['start'] ?? 0;
		}
	}

	/**
	 * Get current URL
	 *
	 * @return string|null Current URL
	 */
	public function current(): ?string {
		$base_url = $this->config['url'];
		$param = $this->config['pagination']['param'] ?? 'page';

		$separator = str_contains( $base_url, '?' ) ? '&' : '?';
		return $base_url . $separator . $param . '=' . $this->position;
	}

	/**
	 * Get current position
	 *
	 * @return int Current position
	 */
	public function key(): int {
		return $this->position;
	}

	/**
	 * Move to next position
	 *
	 * @return void
	 */
	public function next(): void {
		$increment = $this->config['pagination']['increment'] ?? 1;
		$this->position += $increment;
	}

	/**
	 * Reset iterator
	 *
	 * @return void
	 */
	public function rewind(): void {
		$this->position = $this->config['pagination']['start'] ?? 0;
	}

	/**
	 * Check if current position is valid
	 *
	 * @return bool True if position is valid
	 */
	public function valid(): bool {
		if ( ! isset( $this->config['pagination']['max'] ) ) {
			return true;
		}

		return $this->position < $this->config['pagination']['max'];
	}
}
