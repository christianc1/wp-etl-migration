<?php
/**
 * Class: Pipeline
 *
 * Handles the ETL pipeline process by managing pipeline jobs.
 *
 * @package TenupETL\Classes\Pipeline
 */

namespace TenupETL\Classes\Pipeline;

use TenupETL\Classes\Config\GlobalConfig;
use TenupETL\Classes\Config\JobConfig;
use TenupETL\Classes\Load\LedgerRegistry;
use TenupETL\Utils\WithLogging;

/**
 * Pipeline class for managing ETL pipeline jobs.
 */
class Pipeline {
	use WithLogging;

	/**
	 * Global configuration object.
	 *
	 * @var GlobalConfig|null
	 */
	protected $config = null;

	/**
	 * Migration configuration array.
	 *
	 * @var array|null
	 */
	protected $migration = null;

	/**
	 * Pipeline jobs.
	 *
	 * @var array<PipelineJob>
	 */
	protected $pipeline_jobs = [];

	/**
	 * Ledger registry instance
	 *
	 * @var LedgerRegistry
	 */
	protected $ledger_registry;

	/**
	 * Constructor
	 *
	 * @param GlobalConfig $global_config Global configuration object.
	 */
	public function __construct( GlobalConfig $global_config ) {
		$this->config          = $global_config;
		$this->migration       = $this->config->get_value( 'migration' );
		$this->ledger_registry = new LedgerRegistry(
			$global_config
		);
	}

	/**
	 * Build pipeline jobs from migration configuration.
	 *
	 * @return Pipeline Current pipeline instance.
	 */
	public function build(): Pipeline {
		if ( ! $this->validate_dependencies() ) {
			$this->log( 'Migration dependencies validation failed. Please check the logs above.', 'error' );
			return $this;
		}

		foreach ( $this->migration as $job_config ) {
			if ( isset( $job_config['skip'] ) && $job_config['skip'] ) {
				continue;
			}

			$this->pipeline_jobs[ $job_config['name'] ] = $this->create_job( $this->config, $job_config );
		}

		return $this;
	}

	/**
	 * Process all pipeline jobs.
	 *
	 * @return Pipeline Current pipeline instance.
	 */
	public function process(): Pipeline {
		foreach ( $this->pipeline_jobs as $job ) {
			$job->process();
		}

		return $this;
	}

	/**
	 * Get all pipeline jobs.
	 *
	 * @return array Array of pipeline jobs.
	 */
	public function get_pipeline_jobs(): array {
		return $this->pipeline_jobs;
	}

	/**
	 * Create a new pipeline job.
	 *
	 * @param GlobalConfig $config     Global configuration object.
	 * @param array        $job_config Job configuration array.
	 * @return PipelineJob Created pipeline job instance.
	 */
	public function create_job( $config, $job_config ): PipelineJob {
		$job_config = new JobConfig( $job_config );
		$job        = new PipelineJob( $config, $job_config, $this->ledger_registry );
		$job->build();

		return $job;
	}

	/**
	 * Validate migration dependencies
	 *
	 * Checks for circular dependencies and correct dependency order.
	 *
	 * @return bool True if dependencies are valid, false otherwise.
	 */
	protected function validate_dependencies(): bool {
		$dependencies    = [];
		$migration_order = [];
		$is_valid        = true;

		// Build dependency map
		foreach ( $this->migration as $index => $job_config ) {
			$name                     = $job_config['name'];
			$migration_order[ $name ] = $index;
			$dependencies[ $name ]    = isset( $job_config['depends_on'] ) ?
				(array) $job_config['depends_on'] :
				[];
		}

		// Check for circular dependencies
		if ( $this->has_circular_dependencies( $dependencies ) ) {
			$this->log( 'Circular dependencies detected in migration configuration', 'error' );
			$is_valid = false;
		}

		// Validate dependency order
		foreach ( $dependencies as $name => $deps ) {
			foreach ( $deps as $dep ) {
				// Check if dependency exists
				if ( ! isset( $migration_order[ $dep ] ) ) {
					$this->log( "Dependency '{$dep}' required by '{$name}' does not exist", 'error' );
					$is_valid = false;
					continue;
				}

				// Check if dependency comes after the dependent migration
				if ( $migration_order[ $dep ] > $migration_order[ $name ] ) {
					$this->log( "Migration '{$name}' depends on '{$dep}' but comes before it", 'error' );
					$is_valid = false;
				}
			}
		}

		return $is_valid;
	}

	/**
	 * Check for circular dependencies in dependency graph
	 *
	 * @param array $dependencies Map of migration names to their dependencies.
	 * @return bool True if circular dependencies exist, false otherwise.
	 */
	protected function has_circular_dependencies( array $dependencies ): bool {
		$visited = [];
		$path    = [];

		foreach ( array_keys( $dependencies ) as $node ) {
			if ( $this->detect_cycle( $node, $dependencies, $visited, $path ) ) {
				$this->log( 'Circular dependency path: ' . implode( ' -> ', $path ), 'error' );
				return true;
			}
		}

		return false;
	}

	/**
	 * Detect cycles in dependency graph using DFS
	 *
	 * @param string $node Current node being checked.
	 * @param array  $dependencies Dependency graph.
	 * @param array  $visited Nodes already fully checked.
	 * @param array  $path Current path being checked.
	 * @return bool True if cycle detected, false otherwise.
	 */
	protected function detect_cycle( string $node, array $dependencies, array &$visited, array &$path ): bool {
		if ( in_array( $node, $path, true ) ) {
			$path[] = $node;
			return true;
		}

		if ( isset( $visited[ $node ] ) ) {
			return false;
		}

		$path[]           = $node;
		$visited[ $node ] = true;

		foreach ( $dependencies[ $node ] as $dependency ) {
			if ( $this->detect_cycle( $dependency, $dependencies, $visited, $path ) ) {
				return true;
			}
		}

		array_pop( $path );
		return false;
	}
}
