<?php
/**
 * Class: Base Transformation Pipeline
 *
 * Base class for transformation pipelines that process data during ETL operations.
 *
 * @package TenupETL\Classes\Transform\TransformationPipelines
 */

namespace TenupETL\Classes\Transform\TransformationPipelines;

use TenupETL\Classes\Config\{GlobalConfig, JobConfig};
use TenupETL\Classes\Load\LedgerRegistry;
use Flow\ETL\DataFrame;

use Flow\ETL\DSL\{from_array};

/**
 * Abstract base class for transformation pipelines.
 *
 * Provides core functionality for data transformation pipelines including state management
 * and lifecycle methods for preparing, running, and finalizing transformations.
 */
abstract class BaseTransformationPipeline implements TransformationPipeline {

	/**
	 * Constructor.
	 *
	 * @param DataFrame      $state           The initial state of the transformation pipeline.
	 * @param GlobalConfig   $config          Global configuration.
	 * @param JobConfig      $job_config      Job configuration.
	 */
	public function __construct( protected DataFrame $state, protected GlobalConfig $config, protected JobConfig $job_config ) {
		$this->uid = time();
	}

	/**
	 * Run the transformation pipeline.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	public function run(): TransformationPipeline {
		return $this;
	}

	/**
	 * Prepare the transformation pipeline before running.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	public function prepare(): TransformationPipeline {
		return $this;
	}

	/**
	 * Finalize the transformation pipeline after running.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	public function finalize(): TransformationPipeline {
		return $this;
	}

	/**
	 * Get the current state of the transformation pipeline.
	 *
	 * @return DataFrame The current state.
	 */
	public function get_current_state(): DataFrame {
		return $this->state;
	}

	/**
	 * Get the final state after running the transformation pipeline.
	 *
	 * @return DataFrame The final state after finalization.
	 */
	public function get_final_state(): DataFrame {
		return $this->finalize()->get_current_state();
	}
}
