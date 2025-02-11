<?php
/**
 * Class: WordPress User Transformation Pipeline
 *
 * Handles transformation of user data during ETL operations.
 * Sets up default user fields and meta data for WordPress users.
 *
 * @package TenupETL\Classes\Transform\TransformationPipelines
 */

namespace TenupETL\Classes\Transform\TransformationPipelines;

use Flow\ETL\DataFrame;
use Flow\ETL\Row;

use function TenupETL\Classes\Transform\Transformers\{simple_transformer};
use function Flow\ETL\DSL\{ref, cast, equal, when, lit, entry, now };

/**
 * WordPress User Pipeline class for transforming user data.
 *
 * Handles transformation of user data into WordPress compatible format,
 * setting up required user fields and meta data.
 */
class WordPressUserPipeline extends BaseTransformationPipeline {

	/**
	 * Default values for user fields.
	 *
	 * @var array<string, string>
	 */
	public $defaults = [
		'role' => 'subscriber',
	];

	/**
	 * Run the user transformation pipeline.
	 *
	 * Executes all user field transformations in sequence.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	public function run(): TransformationPipeline {
		$this
			->prepare()
			->user_login()
			->user_pass()
			->user_email()
			->display_name()
			->first_name()
			->last_name()
			->user_registered()
			->role()
			->user_meta();

		return $this;
	}

	/**
	 * Get default value for a given field.
	 *
	 * @param string $key The field key to get default for.
	 * @return mixed The default value wrapped in lit() or empty lit().
	 */
	protected function get_default( string $key ): mixed {
		return $this->defaults[ $key ] ? lit( $this->defaults[ $key ] ) : lit( '' );
	}

	/**
	 * Transform user login field.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	protected function user_login(): TransformationPipeline {
		$this->state
			->withEntry( 'wp.user_login', lit( '' ) );

		return $this;
	}

	/**
	 * Transform user password field.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	protected function user_pass(): TransformationPipeline {
		return $this;
	}

	/**
	 * Transform user email field.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	protected function user_email(): TransformationPipeline {
		$this->state
			->withEntry( 'wp.user_email', lit( '' ) );

		return $this;
	}

	/**
	 * Transform display name field.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	protected function display_name(): TransformationPipeline {
		$this->state
			->withEntry( 'wp.display_name', lit( '' ) );

		return $this;
	}

	/**
	 * Transform first name field.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	protected function first_name(): TransformationPipeline {
		$this->state
			->withEntry( 'wp.first_name', lit( '' ) );

		return $this;
	}

	/**
	 * Transform last name field.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	protected function last_name(): TransformationPipeline {
		$this->state
			->withEntry( 'wp.last_name', lit( '' ) );

		return $this;
	}

	/**
	 * Transform user registered date field.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	protected function user_registered(): TransformationPipeline {
		$this->state
			->withEntry( 'wp.user_registered', now() );

		return $this;
	}

	/**
	 * Transform user role field.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	protected function role(): TransformationPipeline {
		$this->state
			->withEntry( 'wp.role', $this->get_default( 'role' ) );

		return $this;
	}

	/**
	 * Transform user meta fields.
	 *
	 * @return TransformationPipeline The current pipeline instance.
	 */
	protected function user_meta(): TransformationPipeline {
		$this->state
			->withEntry(
				'wp.user_meta',
				simple_transformer(
					fn() => [
						'etl_pipeline' => static::class,
					]
				)
			);

		return $this;
	}
}
